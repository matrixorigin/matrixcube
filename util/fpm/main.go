package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	start := false
	importStart := false
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > len("package") && line[:len("package")] == "package" {
			fmt.Println(line)
			continue
		}

		if strings.Contains(line, "import (") {
			fmt.Println(line)
			importStart = true
			continue
		}

		if importStart {
			fmt.Println(line)
			if len(line) > 0 && line[0] == ')' {
				importStart = false
			}
			continue
		}

		if strings.Contains(line, "Unmarshal(dAtA []byte) error {") && !strings.Contains(line, "RaftMessage") &&
			!strings.Contains(line, "SnapshotChunk") {
			start = true
			line = strings.ReplaceAll(line, "Unmarshal(dAtA []byte) error {", "FastUnmarshal(dAtA []byte) error {")
		}

		if start {
			// m.Shard = append(m.Shard[:0], dAtA[iNdEx:postIndex]...)
			if strings.Contains(line, "dAtA[iNdEx:postIndex]...") {
				line = fmt.Sprintf("%s = dAtA[iNdEx:postIndex]", strings.Split(line, " = ")[0])
			}

			// .Unmarshal(dAtA[iNdEx:postIndex])
			if strings.Contains(line, ".Unmarshal(dAtA[iNdEx:postIndex])") {
				line = strings.ReplaceAll(line, ".Unmarshal(dAtA[iNdEx:postIndex])",
					".FastUnmarshal(dAtA[iNdEx:postIndex])")
			}

			fmt.Println(line)

			if len(line) > 0 && line[0] == '}' {
				start = false
			}
		}
	}
}
