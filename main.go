package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/viper"
)

// StorageConfig represents the configuration for a storage provider
type StorageConfig struct {
	Bucket       string `mapstructure:"bucket"`
	Region       string `mapstructure:"region"`
	Endpoint     string `mapstructure:"endpoint"`
	AccessKey    string `mapstructure:"access_key"`
	SecretKey    string `mapstructure:"secret_key"`
	PathPrefix   string `mapstructure:"path_prefix"`
	UsePathStyle bool   `mapstructure:"use_path_style"` // Required for MinIO
}

// ProxyConfig represents the main proxy configuration
type ProxyConfig struct {
	Domains map[string]StorageConfig `mapstructure:"domains"`
	Port    string                   `mapstructure:"port"`
}

// S3Proxy represents the S3 proxy server
type S3Proxy struct {
	config    *ProxyConfig
	s3Clients map[string]*s3.Client
}

// NewS3Proxy creates a new S3 proxy instance
func NewS3Proxy(configFile string) (*S3Proxy, error) {
	config, err := loadConfig(configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %v", err)
	}

	proxy := &S3Proxy{
		config:    config,
		s3Clients: make(map[string]*s3.Client),
	}

	// Create S3 client for each domain
	for domain, storageConfig := range config.Domains {
		client, err := proxy.createS3Client(storageConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create S3 client for %s: %v", domain, err)
		}
		proxy.s3Clients[domain] = client
	}

	return proxy, nil
}

// loadConfig loads configuration using viper
func loadConfig(configFile string) (*ProxyConfig, error) {
	v := viper.NewWithOptions(viper.KeyDelimiter("::"))
	v.SetConfigFile(configFile)

	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	var config ProxyConfig
	if err := v.Unmarshal(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

// createS3Client creates an S3 client with the given configuration
func (p *S3Proxy) createS3Client(cfg StorageConfig) (*s3.Client, error) {
	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.UsePathStyle
	})

	return client, nil
}

// Error writes an error response to the client
func (p *S3Proxy) Error(w http.ResponseWriter, code int) {
	http.Error(w, http.StatusText(code), code)
}

// ServeHTTP handles HTTP requests
func (p *S3Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Only support GET and HEAD requests
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		p.Error(w, http.StatusMethodNotAllowed)
		return
	}

	// Extract domain (remove port if present)
	domain := strings.Split(r.Host, ":")[0]

	storageConfig, exists := p.config.Domains[domain]
	if !exists {
		log.Printf("Domain not found: %s", domain)
		p.Error(w, http.StatusForbidden)
		return
	}

	client, ok := p.s3Clients[domain]
	if !ok {
		log.Printf("Failed to create S3 client for %s", domain)
		p.Error(w, http.StatusInternalServerError)
		return
	}

	// Build object path
	objectPath := strings.TrimPrefix(r.URL.Path, "/")
	if storageConfig.PathPrefix != "" {
		objectPath = strings.TrimSuffix(storageConfig.PathPrefix, "/") + "/" + objectPath
	}

	log.Printf("Request: %s %s -> %s/%s", r.Method, r.URL.Path, storageConfig.Bucket, objectPath)

	// Create GetObject request
	input := &s3.GetObjectInput{
		Bucket: aws.String(storageConfig.Bucket),
		Key:    aws.String(objectPath),
	}

	// Support Range requests for resumable downloads
	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		input.Range = aws.String(rangeHeader)
	}

	// Execute S3 request
	result, err := client.GetObject(context.TODO(), input)
	if err != nil {
		p.Error(w, http.StatusNotFound)
		return
	}
	defer result.Body.Close()

	// Set response headers
	p.setResponseHeaders(w, result)

	// HEAD request only returns headers
	if r.Method == http.MethodHead {
		return
	}

	// Copy file content to response
	_, err = io.Copy(w, result.Body)
	if err != nil {
		log.Printf("Failed to copy response body: %v", err)
	}
}

// setResponseHeaders sets the appropriate response headers
func (p *S3Proxy) setResponseHeaders(w http.ResponseWriter, result *s3.GetObjectOutput) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if result.ContentType != nil {
		w.Header().Set("Content-Type", *result.ContentType)
	}
	if result.ContentLength != nil {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", *result.ContentLength))
	}
	if result.ETag != nil {
		w.Header().Set("ETag", *result.ETag)
	}
	if result.LastModified != nil {
		w.Header().Set("Last-Modified", result.LastModified.Format(http.TimeFormat))
	}
	if result.CacheControl != nil {
		w.Header().Set("Cache-Control", *result.CacheControl)
	}
}

// generateSampleConfig generates a sample configuration file
func generateSampleConfig(configFile string) error {
	v := viper.NewWithOptions(viper.KeyDelimiter("::"))

	// Set default values
	v.Set("port", "8080")
	v.Set("domains::example.com::bucket", "<your-s3-bucket>")
	v.Set("domains::example.com::region", "<your-s3-region>")
	v.Set("domains::example.com::endpoint", "<your-s3-endpoint>")
	v.Set("domains::example.com::access_key", "<your-s3-access-key>")
	v.Set("domains::example.com::secret_key", "<your-s3-secret-key>")
	v.Set("domains::example.com::path_prefix", "<your-s3-path-prefix>")
	v.Set("domains::example.com::use_path_style", false)

	if err := v.WriteConfigAs(configFile); err != nil {
		return fmt.Errorf("failed to write config file: %v", err)
	}

	fmt.Printf("Sample configuration file generated: %s\n", configFile)
	return nil
}

func main() {
	var (
		configFile = flag.String("config", "config.yaml", "Path to configuration file")
		mode       = flag.String("mode", "run", "Execution mode: 'init' to generate config, 'run' to start server")
	)
	flag.Parse()

	// Generate sample configuration
	if *mode == "init" {
		if err := generateSampleConfig(*configFile); err != nil {
			log.Fatalf("Failed to generate sample config: %v", err)
		}
		return
	}

	// Initialize S3 proxy
	proxy, err := NewS3Proxy(*configFile)
	if err != nil {
		log.Fatalf("Failed to initialize S3 proxy: %v", err)
	}

	port := proxy.config.Port
	if port == "" {
		port = "8080"
	}

	log.Printf("S3 proxy server starting on port: %s", port)
	for domain := range proxy.config.Domains {
		log.Printf("Configured domain: %s", domain)
	}

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      proxy,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	log.Fatal(server.ListenAndServe())
}
