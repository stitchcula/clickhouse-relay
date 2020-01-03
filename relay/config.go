package relay

import (
	"github.com/naoina/toml"
	"os"
)

type Config struct {
	HTTPRelays []HTTPConfig `toml:"http"`
	//TCPRelays  []TCPConfig  `toml:"tcp"`
}

type HTTPConfig struct {
	// Name identifies the HTTP relay
	Name string `toml:"name"`

	// Addr should be set to the desired listening host:port
	Addr string `toml:"bind-addr"`

	// Set certificate in order to handle HTTPS requests
	SSLCombinedPem string `toml:"ssl-combined-pem"`

	// Persistent buffer path
	BufferDir string `toml:"buffer-dir"`

	// Outputs is a list of backed servers where writes will be forwarded
	Outputs []HTTPOutputConfig `toml:"output"`
}

type HTTPOutputConfig struct {
	// Name of the backend server
	Name string `toml:"name"`

	// Location should be set to the URL of the backend server's write endpoint
	Location string `toml:"location"`

	// Timeout sets a per-backend timeout for write requests. (Default 10s)
	// The format used is the same seen in time.ParseDuration
	Timeout string `toml:"timeout"`

	// Buffer failed writes up to maximum count. (Default 0, retry/buffering disabled)
	//BufferSizeMB int `toml:"buffer-size-mb"`

	// Maximum batch size in KB (Default 512)
	//MaxBatchKB int `toml:"max-batch-kb"`

	// Maximum delay between retry attempts.
	// The format used is the same seen in time.ParseDuration (Default 10s)
	//MaxDelayInterval string `toml:"max-delay-interval"`

	// Skip TLS verification in order to use self signed certificate.
	// WARNING: It's insecure. Use it only for developing and don't use in production.
	SkipTLSVerification bool `toml:"skip-tls-verification"`
}

type TCPConfig struct {
	// Name identifies the TCP relay
	Name string `toml:"name"`

	// Addr is where the TCP relay will listen for packets
	Addr string `toml:"bind-addr"`

	// ReadBuffer sets the socket buffer for incoming connections
	//ReadBuffer int `toml:"read-buffer"`

	// Outputs is a list of backend servers where writes will be forwarded
	Outputs []TCPOutputConfig `toml:"output"`
}

type TCPOutputConfig struct {
	// Name identifies the TCP backend
	Name string `toml:"name"`

	// Location should be set to the host:port of the backend server
	Location string `toml:"location"`
}

// LoadConfigFile parses the specified file into a Config object
func LoadConfigFile(filename string) (cfg Config, err error) {
	f, err := os.Open(filename)
	if err != nil {
		return cfg, err
	}
	defer f.Close()

	return cfg, toml.NewDecoder(f).Decode(&cfg)
}
