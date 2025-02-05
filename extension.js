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
    <title>Mermaid Flowchart with Pan, Zoom & Download</title>
    <script type="module">
        import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
        mermaid.initialize({ startOnLoad: true });
    </script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/svg2png/1.0.4/svg2png.min.js"></script>
    <style>
        /* General Layout */
        body {
            font-family: 'Arial', sans-serif;
            margin: 0;
            padding: 0;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            height: 100vh;
            background: linear-gradient(135deg, #e0e7f1, #f0f4f8);
            box-sizing: border-box;
            overflow: hidden;
        }

        /* Container */
        .container {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            width: 100%;
            max-width: 1200px;
            padding: 20px;
            background-color: #ffffff;
            border-radius: 15px;
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.2);
            text-align: center;
            position: relative;
        }

        /* Heading */
        h1 {
            font-size: 2.5rem;
            color: #007ACC;
            margin-bottom: 15px;
            font-weight: bold;
        }

        /* Zoom & Pan Controls */
        .controls {
            display: flex;
            gap: 10px;
            margin-bottom: 15px;
        }

        /* Button Styling */
        button {
            padding: 10px 15px;
            font-size: 16px;
            font-weight: bold;
            color: white;
            border: none;
            border-radius: 8px;
            cursor: pointer;
            transition: 0.3s;
        }

        .zoom-in { background-color: #28a745; }
        .zoom-out { background-color: #dc3545; }
        .download-btn { background-color: #007bff; }
        .reset-btn { background-color: #f39c12; }

        button:hover { opacity: 0.8; }
        button:active { transform: scale(0.95); }

        /* Chart Wrapper for Panning */
        .chart-container {
            width: 100%;
            height: 600px;
            overflow: hidden;
            border: 2px solid #007ACC;
            border-radius: 10px;
            position: relative;
            cursor: grab;
        }

        .chart-container:active {
            cursor: grabbing;
        }

        .chart-wrapper {
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            display: flex;
            align-items: center;
            justify-content: center;
        }

        .mermaid {
            transform-origin: 0 0;
            transition: transform 0.2s ease-in-out;
        }
    </style>
</head>
<body>

    <div class="container">
        <h1>Mermaid Flowchart</h1>

        <!-- Zoom & Pan Controls -->
        <div class="controls">
            <button class="zoom-in" onclick="zoomIn()">Zoom In +</button>
            <button class="zoom-out" onclick="zoomOut()">Zoom Out -</button>
            <button class="reset-btn" onclick="resetView()">Reset View</button>
            <button class="download-btn" onclick="downloadChart()">Download</button>
        </div>

        <!-- Chart Container for Panning -->
        <div class="chart-container" id="chart-container">
            <div class="chart-wrapper" id="chart-wrapper">
                <div class="mermaid" id="mermaid-chart">
                    ${escapeHtml(mermaidSnip)}
                </div>
            </div>
        </div>
    </div>

    <script>
        let scale = 1.0;
        let posX = 0, posY = 0;
        let isPanning = false, startX, startY;
        const chartWrapper = document.getElementById("chart-wrapper");

        // Zoom Functions
        function zoomIn() {
            scale += 0.1;
            updateTransform();
        }

        function zoomOut() {
            if (scale > 0.5) {
                scale -= 0.1;
                updateTransform();
            }
        }

        function resetView() {
            scale = 1.0;
            posX = 0;
            posY = 0;
            updateTransform();
        }

        function updateTransform() {
            chartWrapper.style.transform = \`translate(\${posX}px, \${posY}px) scale(\${scale})\`;
        }

        // Pan Functionality
        document.getElementById("chart-container").addEventListener("mousedown", (event) => {
            isPanning = true;
            startX = event.clientX - posX;
            startY = event.clientY - posY;
        });

        document.addEventListener("mousemove", (event) => {
            if (isPanning) {
                posX = event.clientX - startX;
                posY = event.clientY - startY;
                updateTransform();
            }
        });

        document.addEventListener("mouseup", () => isPanning = false);

        // Download Chart
        function downloadChart() {
    const svgElement = document.querySelector(".mermaid svg");

    if (!svgElement) {
        alert("Chart not loaded yet. Please try again.");
        return;
    }

    // Clone the SVG to modify it without affecting the UI
    const clonedSvg = svgElement.cloneNode(true);
    clonedSvg.setAttribute("xmlns", "http://www.w3.org/2000/svg");

    // Get the bounding box of the actual graph (ignoring white space)
    const bbox = svgElement.getBBox();
    clonedSvg.setAttribute("viewBox", bbox.x + " " + bbox.y + " " + bbox.width + " " + bbox.height);
    clonedSvg.setAttribute("width", bbox.width);
    clonedSvg.setAttribute("height", bbox.height);

    // Convert SVG to a PNG
    const svgString = new XMLSerializer().serializeToString(clonedSvg);
    const canvas = document.createElement("canvas");
    const ctx = canvas.getContext("2d");
    const img = new Image();

    img.onload = function () {
        canvas.width = bbox.width;
        canvas.height = bbox.height;
        ctx.drawImage(img, 0, 0, bbox.width, bbox.height);
        const pngFile = canvas.toDataURL("image/png");

        let link = document.createElement("a");
        link.href = pngFile;
        link.download = "mermaid-chart.png";
        link.click();
    };

    img.src = "data:image/svg+xml;base64," + btoa(unescape(encodeURIComponent(svgString)));
}

    </script>

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
