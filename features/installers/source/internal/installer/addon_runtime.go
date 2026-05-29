//go:build linux

package installer

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	addonRuntimeRoot = "/opt/lakehouse/addons"
	addonConfigRoot  = "/etc/lakehouse/addons"
)

func (i *installer) addonAirflowDagsDir() string {
	return filepath.Join(i.baseHome, "airflow", "dags", "addons")
}

func (i *installer) ensureAddonRuntime() error {
	section("ADD-ON RUNTIME")

	paths := []string{
		addonRuntimeRoot,
		filepath.Join(addonConfigRoot, "installed"),
		filepath.Join(addonConfigRoot, "enabled"),
		filepath.Join(addonConfigRoot, "config"),
	}
	for _, path := range paths {
		if err := runElevatedCommand(os.Stdin, os.Stdout, os.Stderr, "mkdir", "-p", path); err != nil {
			return fmt.Errorf("failed to create add-on runtime path %s: %w", path, err)
		}
		if err := runElevatedCommand(os.Stdin, os.Stdout, os.Stderr, "chmod", "755", path); err != nil {
			return fmt.Errorf("failed to set add-on runtime permissions on %s: %w", path, err)
		}
		fmt.Printf("  - Ready: %s\n", path)
	}

	airflowDir := i.addonAirflowDagsDir()
	if err := runElevatedCommand(os.Stdin, os.Stdout, os.Stderr, "mkdir", "-p", airflowDir); err != nil {
		return fmt.Errorf("failed to create airflow add-on DAG path %s: %w", airflowDir, err)
	}
	if err := runElevatedCommand(os.Stdin, os.Stdout, os.Stderr, "chown", fmt.Sprintf("%s:%s", i.currentUser, i.currentGroup), airflowDir); err != nil {
		return fmt.Errorf("failed to set airflow add-on DAG ownership on %s: %w", airflowDir, err)
	}
	if err := runElevatedCommand(os.Stdin, os.Stdout, os.Stderr, "chmod", "755", airflowDir); err != nil {
		return fmt.Errorf("failed to set airflow add-on DAG permissions on %s: %w", airflowDir, err)
	}
	fmt.Printf("  - Ready: %s\n", airflowDir)

	if err := i.ensureAddonTemplateLibrary(); err != nil {
		return err
	}

	i.addSummary("namenode", "add-on runtime", statusOK, "prepared addon runtime directories, airflow addon DAG path, and template library")
	return nil
}
