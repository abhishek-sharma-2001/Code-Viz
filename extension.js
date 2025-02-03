const vscode = require("vscode");
const fs = require("fs");
const path = require("path");
const {
  resolveDependencies,
  convertMapToMermaid,
  extractFunctionFromPosition,
} = require("./functionCallExtractor"); // Importing the functionCallExtractor module

function activate(context) {
  let disposable = vscode.commands.registerCommand(
    "extension.showMermaidFlowchart",
    async (args) => {
      const editor = vscode.window.activeTextEditor;
      if (!editor) {
        vscode.window.showErrorMessage("No active editor found.");
        return;
      }

      const selection = editor.selection;
      const selectedText = editor.document.getText(selection);
      const filePath = editor.document.uri.fsPath;

      let functionName = selectedText.trim();
      let isFullFlow = false; // Flag to determine if it's a full flowchart

      if (!functionName) {
        // No function name selected, generate full flowchart for the entire file
        isFullFlow = true;  // Indicating that we're showing the full flowchart
        const infoMessage = vscode.window.showInformationMessage("No specific function selected. Showing flowchart for all functions.");

        // Auto-close the message after 3 seconds by using setTimeout
        setTimeout(() => {
          // Hide the message manually by using the 'infoMessage' object reference
          infoMessage.then(() => { }); // Do nothing here, just close it
        }, 3000); // Hide after 3 seconds
      } else {
        // User has selected a function name
        functionName = selectedText.trim();
      }

      // Generate the Mermaid diagram
      const mermaidSnip = await generateMermaidDiagram(
        filePath,
        functionName,
        isFullFlow
      );

      if (!mermaidSnip) {
        vscode.window.showErrorMessage("Failed to generate Mermaid diagram.");
        return;
      }

      // Determine the title based on whether it's full flow or a specific function
      const title = isFullFlow ? "Full Flowchart" : `${functionName} Flowchart`;

      // Open WebView with the flowchart
      const panel = vscode.window.createWebviewPanel(
        "mermaidFlowchart",
        title, // Dynamic title based on context
        vscode.ViewColumn.One,
        { enableScripts: true }
      );

      // Set the WebView content
      panel.webview.html = getWebviewContent(mermaidSnip);
    }
  );

  context.subscriptions.push(disposable);
}

async function generateMermaidDiagram(filePath, selectedFunction, isFullFlow) {
  try {
    // Resolve function calls for the given file
    const functionCalls = await resolveDependencies(filePath);

    if (!functionCalls || functionCalls.size === 0) {
      return "graph TD\n  No function calls found"; // Fallback if no function calls
    }

    // If a specific function was selected, generate the diagram for it, including recursive calls
    if (!isFullFlow && selectedFunction) {
      // Include all functions in the call chain for the selected function
      let functionsToInclude = new Set();
      let functionsToProcess = [selectedFunction];

      while (functionsToProcess.length > 0) {
        let currentFunction = functionsToProcess.pop();
        if (!functionsToInclude.has(currentFunction)) {
          functionsToInclude.add(currentFunction);
          const calls = functionCalls.get(currentFunction) || [];
          calls.forEach((calledFunction) => {
            if (!functionsToInclude.has(calledFunction)) {
              functionsToProcess.push(calledFunction);
            }
          });
        }
      }

      // Create a filtered functionCalls map with only the selected function and its dependencies
      const filteredFunctionCalls = new Map();
      functionsToInclude.forEach((func) => {
        filteredFunctionCalls.set(func, functionCalls.get(func) || []);
      });

      return convertMapToMermaid(filteredFunctionCalls);
    }

    // If no function is selected or full flow, generate the diagram for all functions
    const mermaidStr = convertMapToMermaid(functionCalls);
    return mermaidStr;
  } catch (error) {
    console.error("Error generating Mermaid diagram:", error);
    return null;
  }
}

function getWebviewContent(mermaidSnip) {
  return `
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Mermaid Flowchart</title>
        <script type="module">
            import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
            window.addEventListener('load', () => {
                mermaid.initialize({ startOnLoad: true });
                mermaid.init();
            });
        </script>
        <style>
            /* General Layout for the Page */
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                margin: 0;
                padding: 0;
                display: flex;
                justify-content: center;
                align-items: center;
                height: 100vh;
                background: linear-gradient(135deg, #f0f4f8, #e0e7f1); /* Gradient background */
                box-sizing: border-box;
                overflow: hidden;
            }

            /* Container for the flowchart content */
            .container {
                display: flex;
                flex-direction: column;
                align-items: center;
                justify-content: center;
                width: 100%;
                max-width: 1200px;
                padding: 20px;
                background-color: #ffffff;
                border-radius: 12px;
                box-shadow: 0 20px 30px rgba(0, 0, 0, 0.1);
                overflow: hidden;
                text-align: center;
                max-height: 95vh;
            }

            /* Heading Style */
            h1 {
                font-size: 2.5rem;
                color: #007ACC;
                margin-bottom: 30px;
                font-weight: bold;
                letter-spacing: 2px;
                text-shadow: 1px 1px 3px rgba(0, 0, 0, 0.1);
            }

            /* Responsiveness for smaller screens */
            @media (max-width: 768px) {
                h1 {
                    font-size: 2rem;
                }

                .mermaid {
                    height: 400px;
                }
            }

            @media (max-width: 480px) {
                h1 {
                    font-size: 1.5rem;
                }

                .mermaid {
                    height: 300px;
                }
            }

            /* Styling for Mermaid chart */
            .mermaid {
                width: 100%;
                height: 600px;
                overflow: auto;
                display: block;
                margin: 20px 0;
            }

            /* Mermaid Specific Styles (Customizing Node and Arrow Styles) */
            .mermaid .node rect {
                fill: #4e73df; /* Blue Background */
                stroke: #2c3e50; /* Darker border */
                stroke-width: 2px;
                rx: 5px; /* Rounded corners */
                ry: 5px;
            }

            .mermaid .node text {
                font-family: 'Arial', sans-serif;
                font-size: 16px;
                fill: #ffffff; /* White text color */
                font-weight: bold;
                text-align: center;
                letter-spacing: 0.5px;
                text-shadow: 1px 1px 3px rgba(0, 0, 0, 0.2); /* Light text shadow */
                word-wrap: break-word;
                white-space: normal;
                padding: 8px; /* Padding for readability */
            }

            /* Customizing edges (arrows between functions) */
            .mermaid .edgePath .path {
                stroke: #2ecc71; /* Green Arrows */
                stroke-width: 3px;
                fill: none;
                marker-end: url(#arrowhead);
            }

            .mermaid .edgePath .path:hover {
                stroke: #1abc9c; /* Hover effect for arrows */
                stroke-width: 4px;
            }

            .mermaid .edgeLabel {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                font-size: 14px;
                fill: #e74c3c; /* Red color for edge labels */
                font-weight: bold;
            }

            .mermaid .arrowheadPath {
                fill: #2ecc71; /* Green Arrowheads */
            }

            /* Add a custom marker for arrowheads */
            svg defs {
                display: block;
            }

            svg defs marker {
                fill: #2ecc71;
                stroke: #1abc9c;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Mermaid Flowchart</h1>
            <div class="mermaid">
                ${escapeHtml(mermaidSnip)}
            </div>
        </div>
    </body>
    </html>
  `;
}



function escapeHtml(text) {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

module.exports = { activate };
