//go:build linux

package installer

import (
	"embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

//go:embed assets/addon_templates/**
var addonTemplateAssets embed.FS

func (i *installer) ensureAddonTemplateLibrary() error {
	section("ADD-ON TEMPLATES")

	runtimeTemplateRoot := filepath.Join(addonRuntimeRoot, "templates")
	airflowTemplateRoot := filepath.Join(i.addonAirflowDagsDir(), "templates")
	if err := runElevatedCommand(os.Stdin, os.Stdout, os.Stderr, "mkdir", "-p", runtimeTemplateRoot); err != nil {
		return fmt.Errorf("failed to create add-on template root %s: %w", runtimeTemplateRoot, err)
	}
	if err := runElevatedCommand(os.Stdin, os.Stdout, os.Stderr, "mkdir", "-p", airflowTemplateRoot); err != nil {
		return fmt.Errorf("failed to create airflow add-on template root %s: %w", airflowTemplateRoot, err)
	}

	tempRoot, err := os.MkdirTemp("", "lakehouse-addon-templates-*")
	if err != nil {
		return fmt.Errorf("failed to create temp add-on template staging dir: %w", err)
	}
	defer os.RemoveAll(tempRoot)

	if err := materializeEmbeddedTree(addonTemplateAssets, "assets/addon_templates", tempRoot); err != nil {
		return fmt.Errorf("failed to materialize embedded add-on templates: %w", err)
	}

	if err := runElevatedCommand(os.Stdin, os.Stdout, os.Stderr, "rsync", "-a", "--delete", filepath.Clean(tempRoot)+"/", filepath.Clean(runtimeTemplateRoot)+"/"); err != nil {
		return fmt.Errorf("failed to sync add-on templates into %s: %w", runtimeTemplateRoot, err)
	}
	if err := runElevatedCommand(os.Stdin, os.Stdout, os.Stderr, "chmod", "-R", "a+rX", runtimeTemplateRoot); err != nil {
		return fmt.Errorf("failed to normalize add-on template permissions on %s: %w", runtimeTemplateRoot, err)
	}
	fmt.Printf("  - Ready: %s\n", runtimeTemplateRoot)

	if err := runElevatedCommand(os.Stdin, os.Stdout, os.Stderr, "rsync", "-a", "--delete", filepath.Clean(tempRoot)+"/", filepath.Clean(airflowTemplateRoot)+"/"); err != nil {
		return fmt.Errorf("failed to sync add-on templates into %s: %w", airflowTemplateRoot, err)
	}
	if err := runElevatedCommand(os.Stdin, os.Stdout, os.Stderr, "chown", "-R", fmt.Sprintf("%s:%s", i.currentUser, i.currentGroup), airflowTemplateRoot); err != nil {
		return fmt.Errorf("failed to set airflow template ownership on %s: %w", airflowTemplateRoot, err)
	}
	if err := runElevatedCommand(os.Stdin, os.Stdout, os.Stderr, "chmod", "-R", "a+rX", airflowTemplateRoot); err != nil {
		return fmt.Errorf("failed to normalize airflow template permissions on %s: %w", airflowTemplateRoot, err)
	}
	fmt.Printf("  - Ready: %s\n", airflowTemplateRoot)

	airflowIgnorePath := filepath.Join(i.addonAirflowDagsDir(), ".airflowignore")
	ignoreContent := []byte("templates\n")
	if err := os.WriteFile(airflowIgnorePath, ignoreContent, 0o644); err != nil {
		return fmt.Errorf("failed to write airflow ignore file %s: %w", airflowIgnorePath, err)
	}
	fmt.Printf("  - Ready: %s\n", airflowIgnorePath)

	i.addSummary("namenode", "add-on templates", statusOK, "staged example add-on templates and airflow-visible template copy")
	return nil
}

func (i *installer) addonTemplateReadmePath() string {
	return filepath.Join(i.addonAirflowDagsDir(), "templates", "README.md")
}

func (i *installer) printAddonTemplateReadme() error {
	data, err := os.ReadFile(i.addonTemplateReadmePath())
	if err != nil {
		return fmt.Errorf("failed to read add-on template README at %s: %w", i.addonTemplateReadmePath(), err)
	}
	fmt.Println()
	fmt.Printf("=== ADD-ON TEMPLATE README (%s) ===\n", i.addonTemplateReadmePath())
	fmt.Println()
	fmt.Print(string(data))
	if len(data) == 0 || data[len(data)-1] != '\n' {
		fmt.Println()
	}
	return nil
}

func materializeEmbeddedTree(sourceFS embed.FS, sourceRoot, destRoot string) error {
	return fs.WalkDir(sourceFS, sourceRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(sourceRoot, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		target := filepath.Join(destRoot, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}

		data, err := sourceFS.ReadFile(path)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
}
