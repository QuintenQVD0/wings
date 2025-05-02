package diagnostics

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	dockerSystem "github.com/docker/docker/api/types/system"
	"github.com/docker/docker/pkg/parsers/kernel"
	"github.com/docker/docker/pkg/parsers/operatingsystem"

	"github.com/pelican-dev/wings/config"
	"github.com/pelican-dev/wings/environment"
	"github.com/pelican-dev/wings/system"
)

type DiagnosticsArgs struct {
	IncludeEndpoints   bool
	IncludeLogs        bool
	LogLines           int
}

func GenerateDiagnosticsReport(args DiagnosticsArgs) (string, error) {
	dockerVersion, dockerInfo, dockerErr := getDockerInfo()

	output := &strings.Builder{}
	fmt.Fprintln(output, "Pelican Wings - Diagnostics Report")
	printHeader(output, "Versions")
	fmt.Fprintln(output, "               Wings:", system.Version)
	if dockerErr == nil {
		fmt.Fprintln(output, "              Docker:", dockerVersion.Version)
	}
	if v, err := kernel.GetKernelVersion(); err == nil {
		fmt.Fprintln(output, "              Kernel:", v)
	}
	if os, err := operatingsystem.GetOperatingSystem(); err == nil {
		fmt.Fprintln(output, "                  OS:", os)
	}

	printHeader(output, "Wings Configuration")
	if err := config.FromFile(config.DefaultLocation); err != nil {
	}
	cfg := config.Get()

	fmt.Fprintln(output, "      Panel Location:", redact(cfg.PanelLocation, args))
	fmt.Fprintln(output, "")
	fmt.Fprintln(output, "  Internal Webserver:", redact(cfg.Api.Host, args), ":", cfg.Api.Port)
	fmt.Fprintln(output, "         SSL Enabled:", cfg.Api.Ssl.Enabled)
	fmt.Fprintln(output, "     SSL Certificate:", redact(cfg.Api.Ssl.CertificateFile, args))
	fmt.Fprintln(output, "             SSL Key:", redact(cfg.Api.Ssl.KeyFile, args))
	fmt.Fprintln(output, "")
	fmt.Fprintln(output, "         SFTP Server:", redact(cfg.System.Sftp.Address, args), ":", cfg.System.Sftp.Port)
	fmt.Fprintln(output, "      SFTP Read-Only:", cfg.System.Sftp.ReadOnly)
	fmt.Fprintln(output, "")
	fmt.Fprintln(output, "      Root Directory:", cfg.System.RootDirectory)
	fmt.Fprintln(output, "      Logs Directory:", cfg.System.LogDirectory)
	fmt.Fprintln(output, "      Data Directory:", cfg.System.Data)
	fmt.Fprintln(output, "   Archive Directory:", cfg.System.ArchiveDirectory)
	fmt.Fprintln(output, "    Backup Directory:", cfg.System.BackupDirectory)
	fmt.Fprintln(output, "")
	fmt.Fprintln(output, "            Username:", cfg.System.Username)
	fmt.Fprintln(output, "         Server Time:", time.Now().Format(time.RFC1123Z))
	fmt.Fprintln(output, "          Debug Mode:", cfg.Debug)

	printHeader(output, "Docker: Info")
	if dockerErr == nil {
		fmt.Fprintln(output, "Server Version:", dockerInfo.ServerVersion)
		fmt.Fprintln(output, "Storage Driver:", dockerInfo.Driver)
		if dockerInfo.DriverStatus != nil {
			for _, pair := range dockerInfo.DriverStatus {
				fmt.Fprintf(output, "  %s: %s\n", pair[0], pair[1])
			}
		}
		if dockerInfo.SystemStatus != nil {
			for _, pair := range dockerInfo.SystemStatus {
				fmt.Fprintf(output, " %s: %s\n", pair[0], pair[1])
			}
		}
		fmt.Fprintln(output, "LoggingDriver:", dockerInfo.LoggingDriver)
		fmt.Fprintln(output, " CgroupDriver:", dockerInfo.CgroupDriver)
		if len(dockerInfo.Warnings) > 0 {
			for _, w := range dockerInfo.Warnings {
				fmt.Fprintln(output, w)
			}
		}
	} else {
		fmt.Fprintln(output, dockerErr.Error())
	}

	printHeader(output, "Docker: Running Containers")
	if co, err := exec.Command("docker", "ps").Output(); err == nil {
		output.Write(co)
	} else {
		fmt.Fprint(output, "Couldn't list containers: ", err)
	}

	printHeader(output, "Latest Wings Logs")
	if args.IncludeLogs {
		p := "/var/log/pelican/wings.log"
		if cfg != nil {
			p = path.Join(cfg.System.LogDirectory, "wings.log")
		}
		if c, err := exec.Command("tail", "-n", strconv.Itoa(args.LogLines), p).Output(); err != nil {
			fmt.Fprintln(output, "No logs found or an error occurred.")
		} else {
			fmt.Fprintf(output, "%s\n", string(c))
		}
	} else {
		fmt.Fprintln(output, "Logs redacted.")
	}

	if !args.IncludeEndpoints {
		s := output.String()
		output.Reset()
		s = strings.ReplaceAll(s, cfg.PanelLocation, "{redacted}")
		s = strings.ReplaceAll(s, cfg.Api.Host, "{redacted}")
		s = strings.ReplaceAll(s, cfg.Api.Ssl.CertificateFile, "{redacted}")
		s = strings.ReplaceAll(s, cfg.Api.Ssl.KeyFile, "{redacted}")
		s = strings.ReplaceAll(s, cfg.System.Sftp.Address, "{redacted}")
		output.WriteString(s)
	}

	return output.String(), nil
}

func redact(s string, args DiagnosticsArgs) string {
	if !args.IncludeEndpoints {
		return "{redacted}"
	}
	return s
}

func printHeader(w io.Writer, title string) {
	fmt.Fprintln(w, "\n|\n|", title)
	fmt.Fprintln(w, "| ------------------------------")
}

func getDockerInfo() (types.Version, dockerSystem.Info, error) {
	client, err := environment.Docker()
	if err != nil {
		return types.Version{}, dockerSystem.Info{}, err
	}
	dockerVersion, err := client.ServerVersion(context.Background())
	if err != nil {
		return types.Version{}, dockerSystem.Info{}, err
	}
	dockerInfo, err := client.Info(context.Background())
	if err != nil {
		return types.Version{}, dockerSystem.Info{}, err
	}
	return dockerVersion, dockerInfo, nil
}
