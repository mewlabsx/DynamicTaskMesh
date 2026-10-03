package config

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	capabilitycatalog "dtm/internal/capability"
	"dtm/internal/model"
	"gopkg.in/yaml.v3"

	"dtm/internal/mesh/discovery"
	"dtm/internal/mesh/handshake"
	"dtm/internal/mesh/membership"
)

const (
	DefaultLeaseTTL               = 10 * time.Second
	DefaultLeaseSweepInterval     = time.Second
	DefaultHeartbeatInterval      = 2 * time.Second
	DefaultRetryMaxAttempts       = 3
	DefaultRetryBackoff           = 100 * time.Millisecond
	DefaultStorageDriver          = "sqlite"
	DefaultJournalMode            = "WAL"
	DefaultSynchronous            = "NORMAL"
	DefaultBusyTimeout            = 5 * time.Second
	DefaultMeshProtocolMajor      = 1
	DefaultMeshProtocolMinor      = 0
	DefaultMulticastGroup         = "239.255.42.99"
	DefaultMulticastPort          = 45892
	DefaultDiscoveryInterval      = time.Second
	DefaultMembershipSuspectAfter = membership.DefaultSuspectAfter
	DefaultMembershipExpireAfter  = membership.DefaultExpireAfter
	ModeStatic                    = "static"
	ModeRuntime                   = "runtime"
)

type Duration struct {
	time.Duration
}

func (duration *Duration) UnmarshalYAML(value *yaml.Node) error {
	parsed, err := time.ParseDuration(strings.TrimSpace(value.Value))
	if err != nil || parsed <= 0 {
		return fmt.Errorf("duration must be positive: %q", value.Value)
	}
	duration.Duration = parsed
	return nil
}

type Endpoint struct {
	Address string `yaml:"address"`
}

type NATS struct {
	URL string `yaml:"url"`
}

type Server struct {
	ListenAddress     string `yaml:"listen_address"`
	Address           string `yaml:"address"`
	AdvertisedAddress string `yaml:"advertised_address"`
}

type Node struct {
	ID               string   `yaml:"id"`
	Capabilities     []string `yaml:"capabilities"`
	AdvertiseAddress string   `yaml:"advertise_address"`
}

type Lease struct {
	TTL           Duration `yaml:"ttl"`
	SweepInterval Duration `yaml:"sweep_interval"`
}

type Heartbeat struct {
	Interval Duration `yaml:"interval"`
}

type MeshDiscovery struct {
	MulticastGroup string   `yaml:"multicast_group"`
	Port           int      `yaml:"port"`
	Interface      string   `yaml:"interface"`
	Interval       Duration `yaml:"interval"`
}

type MeshControl struct {
	Listen    string `yaml:"listen"`
	Advertise string `yaml:"advertise"`
}

type MeshMembership struct {
	SuspectAfter Duration `yaml:"suspect_after"`
	ExpireAfter  Duration `yaml:"expire_after"`
}

type Mesh struct {
	Namespace     string         `yaml:"namespace"`
	ProtocolMajor uint32         `yaml:"protocol_major"`
	ProtocolMinor uint32         `yaml:"protocol_minor"`
	Discovery     MeshDiscovery  `yaml:"discovery"`
	Control       MeshControl    `yaml:"control"`
	Membership    MeshMembership `yaml:"membership"`
}

type Storage struct {
	Driver      string   `yaml:"driver"`
	Path        string   `yaml:"path"`
	JournalMode string   `yaml:"journal_mode"`
	Synchronous string   `yaml:"synchronous"`
	BusyTimeout Duration `yaml:"busy_timeout"`
	AutoMigrate *bool    `yaml:"auto_migrate"`
}

func (storage Storage) AutoMigrationEnabled() bool {
	return storage.AutoMigrate == nil || *storage.AutoMigrate
}

func defaultStorage(path string) Storage {
	autoMigrate := true
	return Storage{
		Driver:      DefaultStorageDriver,
		Path:        path,
		JournalMode: DefaultJournalMode,
		Synchronous: DefaultSynchronous,
		BusyTimeout: Duration{Duration: DefaultBusyTimeout},
		AutoMigrate: &autoMigrate,
	}
}

type Retry struct {
	MaxAttempts int      `yaml:"max_attempts"`
	Backoff     Duration `yaml:"backoff"`
}

type Core struct {
	Server  Endpoint `yaml:"server"`
	NATS    NATS     `yaml:"nats"`
	Lease   Lease    `yaml:"lease"`
	Storage Storage  `yaml:"storage"`
	Retry   Retry    `yaml:"retry"`
}

type Agent struct {
	Mode      string    `yaml:"mode"`
	Node      Node      `yaml:"node"`
	Core      Endpoint  `yaml:"core"`
	Server    Server    `yaml:"server"`
	Storage   Storage   `yaml:"storage"`
	NATS      NATS      `yaml:"nats"`
	Heartbeat Heartbeat `yaml:"heartbeat"`
	Mesh      Mesh      `yaml:"mesh"`
}

func LoadCore(path string) (Core, error) {
	result := Core{
		Lease: Lease{
			TTL:           Duration{Duration: DefaultLeaseTTL},
			SweepInterval: Duration{Duration: DefaultLeaseSweepInterval},
		},
		Storage: defaultStorage("dtm.db"),
		Retry: Retry{
			MaxAttempts: DefaultRetryMaxAttempts,
			Backoff:     Duration{Duration: DefaultRetryBackoff},
		},
	}
	if err := load(path, &result); err != nil {
		return Core{}, err
	}
	result.Server.Address = strings.TrimSpace(result.Server.Address)
	if err := normalizeStorage("core", &result.Storage); err != nil {
		return Core{}, err
	}
	if strings.TrimSpace(result.Server.Address) == "" {
		return Core{}, fmt.Errorf("validate core config: server.address is required")
	}
	if result.Retry.MaxAttempts < 1 {
		return Core{}, fmt.Errorf("validate core config: retry.max_attempts must be positive")
	}
	if result.Storage.Path != ":memory:" && !filepath.IsAbs(result.Storage.Path) {
		absoluteConfig, err := filepath.Abs(path)
		if err != nil {
			return Core{}, fmt.Errorf("resolve core config path: %w", err)
		}
		result.Storage.Path = filepath.Clean(filepath.Join(filepath.Dir(absoluteConfig), result.Storage.Path))
	}
	return result, nil
}

func LoadAgent(path string) (Agent, error) {
	result := Agent{
		Mode:    ModeStatic,
		Storage: defaultStorage("agent.db"),
		Heartbeat: Heartbeat{
			Interval: Duration{Duration: DefaultHeartbeatInterval},
		},
		Mesh: Mesh{
			ProtocolMajor: DefaultMeshProtocolMajor,
			ProtocolMinor: DefaultMeshProtocolMinor,
			Discovery:     MeshDiscovery{MulticastGroup: DefaultMulticastGroup, Port: DefaultMulticastPort, Interval: Duration{Duration: DefaultDiscoveryInterval}},
			Membership:    MeshMembership{SuspectAfter: Duration{Duration: DefaultMembershipSuspectAfter}, ExpireAfter: Duration{Duration: DefaultMembershipExpireAfter}},
		},
	}
	if err := load(path, &result); err != nil {
		return Agent{}, err
	}
	result.Node.ID = strings.TrimSpace(result.Node.ID)
	result.Mode = strings.ToLower(strings.TrimSpace(result.Mode))
	result.Core.Address = strings.TrimSpace(result.Core.Address)
	result.Server.ListenAddress = strings.TrimSpace(result.Server.ListenAddress)
	result.Server.Address = strings.TrimSpace(result.Server.Address)
	result.Server.AdvertisedAddress = strings.TrimSpace(result.Server.AdvertisedAddress)
	result.Node.AdvertiseAddress = strings.TrimSpace(result.Node.AdvertiseAddress)
	result.Mesh.Namespace = strings.TrimSpace(result.Mesh.Namespace)
	result.Mesh.Discovery.MulticastGroup = strings.TrimSpace(result.Mesh.Discovery.MulticastGroup)
	result.Mesh.Discovery.Interface = strings.TrimSpace(result.Mesh.Discovery.Interface)
	result.Mesh.Control.Listen = strings.TrimSpace(result.Mesh.Control.Listen)
	result.Mesh.Control.Advertise = strings.TrimSpace(result.Mesh.Control.Advertise)
	if err := normalizeStorage("agent", &result.Storage); err != nil {
		return Agent{}, err
	}

	listenAddress, err := resolveRenamedField(
		"server.listen_address",
		result.Server.ListenAddress,
		"server.address",
		result.Server.Address,
	)
	if err != nil {
		return Agent{}, err
	}
	advertiseAddress, err := resolveRenamedField(
		"node.advertise_address",
		result.Node.AdvertiseAddress,
		"server.advertised_address",
		result.Server.AdvertisedAddress,
	)
	if err != nil {
		return Agent{}, err
	}
	result.Server.ListenAddress = listenAddress
	result.Server.Address = listenAddress
	result.Node.AdvertiseAddress = advertiseAddress
	result.Server.AdvertisedAddress = advertiseAddress

	if result.Node.ID == "" {
		return Agent{}, fmt.Errorf("validate agent config: node.id is required")
	}
	if result.Mode != ModeStatic && result.Mode != ModeRuntime {
		return Agent{}, fmt.Errorf("validate agent config: mode must be %q or %q", ModeStatic, ModeRuntime)
	}
	capabilities, err := normalizeCapabilities(result.Node.Capabilities, result.Mode == ModeStatic)
	if err != nil {
		return Agent{}, err
	}
	result.Node.Capabilities = capabilities
	if result.Mode == ModeStatic && result.Core.Address == "" {
		return Agent{}, fmt.Errorf("validate agent config: core.address is required")
	}
	if result.Mode == ModeRuntime {
		if err := validateMesh(result.Mesh); err != nil {
			return Agent{}, err
		}
	}
	if result.Server.ListenAddress == "" {
		return Agent{}, fmt.Errorf("validate agent config: server.listen_address is required")
	}
	if result.Node.AdvertiseAddress == "" {
		return Agent{}, fmt.Errorf("validate agent config: node.advertise_address is required")
	}
	if result.Storage.Path != ":memory:" && !filepath.IsAbs(result.Storage.Path) {
		absoluteConfig, err := filepath.Abs(path)
		if err != nil {
			return Agent{}, fmt.Errorf("resolve agent config path: %w", err)
		}
		result.Storage.Path = filepath.Clean(
			filepath.Join(filepath.Dir(absoluteConfig), result.Storage.Path),
		)
	}
	return result, nil
}

func validateMesh(mesh Mesh) error {
	if mesh.Namespace == "" {
		return fmt.Errorf("validate agent config: mesh.namespace is required in runtime mode")
	}
	if mesh.ProtocolMajor == 0 {
		return fmt.Errorf("validate agent config: mesh.protocol_major must be positive")
	}
	ip := net.ParseIP(mesh.Discovery.MulticastGroup)
	if ip == nil || ip.To4() == nil || !ip.IsMulticast() {
		return fmt.Errorf("validate agent config: mesh.discovery.multicast_group must be an IPv4 multicast address")
	}
	if mesh.Discovery.Port < 1024 || mesh.Discovery.Port > 65535 {
		return fmt.Errorf("validate agent config: mesh.discovery.port must be between 1024 and 65535")
	}
	if mesh.Discovery.Interval.Duration <= 0 {
		return fmt.Errorf("validate agent config: mesh.discovery.interval must be positive")
	}
	if mesh.Membership.SuspectAfter.Duration <= 0 {
		return fmt.Errorf("validate agent config: mesh.membership.suspect_after must be positive")
	}
	refreshBound, err := discovery.ConfirmedRefreshUpperBound(mesh.Discovery.Interval.Duration, handshake.DefaultTimeout)
	if err != nil || mesh.Membership.SuspectAfter.Duration <= refreshBound {
		return fmt.Errorf("validate agent config: mesh.membership.suspect_after must be greater than the confirmed refresh bound (3 * mesh.discovery.interval + %s)", handshake.DefaultTimeout)
	}
	if mesh.Membership.ExpireAfter.Duration <= mesh.Membership.SuspectAfter.Duration {
		return fmt.Errorf("validate agent config: mesh.membership.expire_after must be greater than suspect_after")
	}
	if mesh.Discovery.Interface != "" {
		interfaceIP := net.ParseIP(mesh.Discovery.Interface)
		if interfaceIP == nil || interfaceIP.To4() == nil || interfaceIP.IsUnspecified() {
			return fmt.Errorf("validate agent config: mesh.discovery.interface must be a local IPv4 address")
		}
	}
	if err := validateHostPort("mesh.control.listen", mesh.Control.Listen); err != nil {
		return err
	}
	if err := validateHostPort("mesh.control.advertise", mesh.Control.Advertise); err != nil {
		return err
	}
	return nil
}

func validateHostPort(name, value string) error {
	if value == "" {
		return fmt.Errorf("validate agent config: %s is required in runtime mode", name)
	}
	host, portText, err := net.SplitHostPort(value)
	if err != nil || strings.TrimSpace(host) == "" {
		return fmt.Errorf("validate agent config: %s must be a host:port endpoint", name)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("validate agent config: %s has an invalid port", name)
	}
	return nil
}

func normalizeStorage(owner string, storage *Storage) error {
	storage.Driver = strings.ToLower(strings.TrimSpace(storage.Driver))
	storage.Path = strings.TrimSpace(storage.Path)
	storage.JournalMode = strings.ToUpper(strings.TrimSpace(storage.JournalMode))
	storage.Synchronous = strings.ToUpper(strings.TrimSpace(storage.Synchronous))
	if storage.Driver == "" {
		storage.Driver = DefaultStorageDriver
	}
	if storage.JournalMode == "" {
		storage.JournalMode = DefaultJournalMode
	}
	if storage.Synchronous == "" {
		storage.Synchronous = DefaultSynchronous
	}
	if storage.BusyTimeout.Duration == 0 {
		storage.BusyTimeout = Duration{Duration: DefaultBusyTimeout}
	}
	if storage.Driver != DefaultStorageDriver {
		return fmt.Errorf("validate %s config: storage.driver must be %q", owner, DefaultStorageDriver)
	}
	if storage.Path == "" {
		return fmt.Errorf("validate %s config: storage.path is required", owner)
	}
	if storage.JournalMode != "WAL" && storage.JournalMode != "DELETE" {
		return fmt.Errorf("validate %s config: unsupported storage.journal_mode %q", owner, storage.JournalMode)
	}
	switch storage.Synchronous {
	case "OFF", "NORMAL", "FULL", "EXTRA":
	default:
		return fmt.Errorf("validate %s config: unsupported storage.synchronous %q", owner, storage.Synchronous)
	}
	return nil
}

func normalizeCapabilities(input []string, required bool) ([]string, error) {
	if len(input) == 0 && required {
		return nil, fmt.Errorf("validate agent config: node.capabilities is required")
	}

	result := make([]string, len(input))
	seen := make(map[string]struct{}, len(input))
	for i, capability := range input {
		capability = strings.TrimSpace(capability)
		if capability == "" {
			return nil, fmt.Errorf("validate agent config: node.capabilities[%d] is required", i)
		}
		if _, exists := seen[capability]; exists {
			return nil, fmt.Errorf("validate agent config: duplicate node capability %q", capability)
		}
		if _, exists := capabilitycatalog.Lookup(model.Capability(capability)); !exists {
			return nil, fmt.Errorf(
				"validate agent config: node.capabilities[%d]: %w %q",
				i,
				capabilitycatalog.ErrUnknownCapability,
				capability,
			)
		}
		seen[capability] = struct{}{}
		result[i] = capability
	}
	return result, nil
}

func resolveRenamedField(
	currentName, currentValue, legacyName, legacyValue string,
) (string, error) {
	if currentValue != "" && legacyValue != "" && currentValue != legacyValue {
		return "", fmt.Errorf(
			"validate agent config: %s conflicts with deprecated %s",
			currentName,
			legacyName,
		)
	}
	if currentValue != "" {
		return currentValue, nil
	}
	return legacyValue, nil
}

func load(path string, target any) error {
	contents, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config %q: %w", path, err)
	}
	if err := yaml.Unmarshal(contents, target); err != nil {
		return fmt.Errorf("parse config %q: %w", path, err)
	}
	return nil
}
