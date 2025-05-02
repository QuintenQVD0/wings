package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"

	"github.com/AlecAivazis/survey/v2"
	"github.com/AlecAivazis/survey/v2/terminal"
	"github.com/apex/log"
	"github.com/goccy/go-json"
	"github.com/pelican-dev/wings/loggers/cli"
	"github.com/spf13/cobra"
)

const (
	DefaultHastebinUrl = "https://paste.pelistuff.com"
	DefaultLogLines    = 200
)

type DiagnosticsArgs struct {
	IncludeEndpoints   bool
	IncludeLogs        bool
	ReviewBeforeUpload bool
	HastebinURL        string
	LogLines           int
}

var diagnosticsArgs DiagnosticsArgs

func newDiagnosticsCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "diagnostics",
		Short: "Collect and report information about this Wings instance to assist in debugging.",
		PreRun: func(cmd *cobra.Command, args []string) {
			initConfig()
			log.SetHandler(cli.Default)
		},
		Run: diagnosticsCmdRun,
	}

	command.Flags().StringVar(&diagnosticsArgs.HastebinURL, "hastebin-url", DefaultHastebinUrl, "the url of the hastebin instance to use")
	command.Flags().IntVar(&diagnosticsArgs.LogLines, "log-lines", DefaultLogLines, "the number of log lines to include in the report")

	return command
}

// diagnosticsCmdRun collects diagnostics about wings, its configuration and the node.
// We collect:
// - wings and docker versions
// - relevant parts of daemon configuration
// - the docker debug output
// - running docker containers
// - logs
func diagnosticsCmdRun(*cobra.Command, []string) {
	questions := []*survey.Question{
		{
			Name:   "IncludeEndpoints",
			Prompt: &survey.Confirm{Message: "Do you want to include endpoints (i.e. the FQDN/IP of your panel)?", Default: false},
		},
		{
			Name:   "IncludeLogs",
			Prompt: &survey.Confirm{Message: "Do you want to include the latest logs?", Default: true},
		},
		{
			Name: "ReviewBeforeUpload",
			Prompt: &survey.Confirm{
				Message: "Do you want to review the collected data before uploading to " + diagnosticsArgs.HastebinURL + "?",
				Help:    "The data, especially the logs, might contain sensitive information, so you should review it. You will be asked again if you want to upload.",
				Default: true,
			},
		},
	}
	if err := survey.Ask(questions, &diagnosticsArgs); err != nil {
		if err == terminal.InterruptErr {
			return
		}
		panic(err)
	}

	report, err := GenerateDiagnosticsReport(diagnosticsArgs)
	if err != nil {
		fmt.Println("Failed to generate report:", err)
		return
	}

	fmt.Println("\n---------------  generated report  ---------------")
	fmt.Println(report)
	fmt.Print("---------------   end of report    ---------------\n\n")

	upload := !diagnosticsArgs.ReviewBeforeUpload
	if !upload {
		survey.AskOne(&survey.Confirm{Message: "Upload to " + diagnosticsArgs.HastebinURL + "?", Default: false}, &upload)
	}
	if upload {
		u, err := uploadToHastebin(diagnosticsArgs.HastebinURL, report)
		if err == nil {
			fmt.Println("Your report is available here: ", u)
		}
	}
}

func GenerateDiagnosticsReport(diagnosticsArgs DiagnosticsArgs) (string, any) {
	panic("unimplemented")
}

func uploadToHastebin(hbUrl, content string) (string, error) {
	u, err := url.Parse(hbUrl)
	if err != nil {
		return "", err
	}

	formData := new(bytes.Buffer)
	formWriter := multipart.NewWriter(formData)
	formWriter.WriteField("c", content)
	formWriter.WriteField("e", "14d")
	formWriter.Close()

	res, err := http.Post(u.String(), formWriter.FormDataContentType(), formData)
	if err != nil || res.StatusCode != 200 {
		fmt.Println("Failed to upload report to ", u.String(), err, res.StatusCode)
		myb, _ := io.ReadAll(res.Body)
		fmt.Println(string(myb))
		return "", err
	}
	pres := make(map[string]interface{})
	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Println("Failed to parse response.", err)
		return "", err
	}
	json.Unmarshal(body, &pres)
	if pasteUrl, ok := pres["url"].(string); ok {
		return pasteUrl, nil
	}
	return "", errors.New("failed to find key in response")
}
