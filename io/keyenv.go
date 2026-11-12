package io

import (
	"fmt"
	"os"
	"strings"
)

func lookupKeyEnv(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("empty environment variable name")
	}
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return "", fmt.Errorf("environment variable [%s] is not set or empty", name)
	}
	return value, nil
}

// resolveKeyEnvFlag sets *plain from the variable named by env; flag is the -env flag's name.
func resolveKeyEnvFlag(flag string, plain **string, env *string) error {
	if env == nil {
		return nil
	}
	value, err := lookupKeyEnv(*env)
	if err != nil {
		return fmt.Errorf("--%s: %w", flag, err)
	}
	*plain = &value
	return nil
}

// resolveColumnKeyEnvFlags appends 'column.path=value' to plain for each 'column.path=ENV_NAME' in env.
func resolveColumnKeyEnvFlags(flag string, plain, env []string) ([]string, error) {
	for _, ck := range env {
		path, name, found := strings.Cut(ck, "=")
		if !found || path == "" || name == "" {
			return nil, fmt.Errorf("invalid --%s format [%s], expected 'column.path=ENV_NAME'", flag, ck)
		}
		value, err := lookupKeyEnv(name)
		if err != nil {
			return nil, fmt.Errorf("--%s [%s]: %w", flag, path, err)
		}
		plain = append(plain, path+"="+value)
	}
	return plain, nil
}

// applyReadKeyEnv resolves the -env flags into their plain counterparts. The
// xor tags on ReadOption keep plain flags, -env flags and --key-file apart.
func applyReadKeyEnv(opt *ReadOption) error {
	if err := resolveKeyEnvFlag("footer-key-env", &opt.FooterKey, opt.FooterKeyEnv); err != nil {
		return err
	}
	if err := resolveKeyEnvFlag("aad-prefix-env", &opt.AADPrefix, opt.AADPrefixEnv); err != nil {
		return err
	}
	columnKeys, err := resolveColumnKeyEnvFlags("column-key-env", opt.ColumnKeys, opt.ColumnKeyEnvs)
	if err != nil {
		return err
	}
	opt.ColumnKeys = columnKeys
	return nil
}

// applyWriteKeyEnv is applyReadKeyEnv for WriteOption.
func applyWriteKeyEnv(opt *WriteOption) error {
	if err := resolveKeyEnvFlag("writer-footer-key-env", &opt.WriterFooterKey, opt.WriterFooterKeyEnv); err != nil {
		return err
	}
	columnKeys, err := resolveColumnKeyEnvFlags("writer-column-key-env", opt.WriterColumnKeys, opt.WriterColumnKeyEnvs)
	if err != nil {
		return err
	}
	opt.WriterColumnKeys = columnKeys
	return nil
}
