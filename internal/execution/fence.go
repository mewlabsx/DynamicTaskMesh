package execution

import "fmt"

// ExecutionFenceMode describes what the execution boundary actually checked.
// It is deliberately separate from an execution-fence verdict: a local
// validator can reject a target without proving an Authority-backed receipt.
type ExecutionFenceMode string

const (
	// ExecutionFenceModeNone means that the execution boundary did not perform
	// fence validation.
	ExecutionFenceModeNone ExecutionFenceMode = "none"
	// ExecutionFenceModeLocalValidator means that the execution boundary ran
	// local structural checks only. It is not an Authority confirmation.
	ExecutionFenceModeLocalValidator ExecutionFenceMode = "local-validator"
	// ExecutionFenceModeAuthorityConfirmed is reserved for a future
	// Authority-backed receipt. M2-R1 must not emit or accept this mode.
	ExecutionFenceModeAuthorityConfirmed ExecutionFenceMode = "authority-confirmed"
)

func (mode ExecutionFenceMode) Validate() error {
	switch mode {
	case "", ExecutionFenceModeNone, ExecutionFenceModeLocalValidator, ExecutionFenceModeAuthorityConfirmed:
		return nil
	default:
		return fmt.Errorf("unknown execution fence mode %q", mode)
	}
}
