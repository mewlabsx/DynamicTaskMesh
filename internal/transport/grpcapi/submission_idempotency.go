package grpcapi

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/task"

	"google.golang.org/protobuf/proto"
)

const submissionFingerprintDomain = "DynamicTaskMesh/SubmitTaskRequest/v1"

var (
	errInvalidIdempotencyKey = errors.New("invalid idempotency key")
	idempotencyKeyPattern    = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
)

func validateIdempotencyKey(key string) error {
	if key == "" {
		return nil
	}
	if !idempotencyKeyPattern.MatchString(key) {
		return errInvalidIdempotencyKey
	}
	return nil
}

func submissionFingerprint(input task.Task) (string, error) {
	requirements := make([]string, len(input.Requirements))
	for index := range input.Requirements {
		requirements[index] = input.Requirements[index].String()
	}
	canonical := &dtmv1.SubmitTaskRequest{Task: &dtmv1.Task{
		Intent: input.Intent, Requirements: requirements, Constraints: input.Constraints,
	}}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(canonical)
	if err != nil {
		return "", err
	}
	digest := sha256.New()
	_, _ = digest.Write([]byte(submissionFingerprintDomain))
	_, _ = digest.Write(encoded)
	return "v1:" + hex.EncodeToString(digest.Sum(nil)), nil
}

func idempotencyKeyHashPrefix(key string) string {
	digest := sha256.Sum256([]byte(key))
	return hex.EncodeToString(digest[:])[:12]
}
