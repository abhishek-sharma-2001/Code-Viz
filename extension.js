const vscode = require('vscode');
const fs = require('fs');
const path = require('path');
const { resolveDependencies, convertMapToMermaid, extractFunctionFromPosition } = require('./functionCallExtractor'); // Importing the functionCallExtractor module

function activate(context) {
    let disposable = vscode.commands.registerCommand('extension.showMermaidFlowchart', async (args) => {
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
            vscode.window.showInformationMessage("No specific function selected. Showing flowchart for all functions.");
        } else {
            // User has selected a function name
            functionName = selectedText.trim();
        }

        // Generate the Mermaid diagram
        const mermaidSnip = await generateMermaidDiagram(filePath, functionName, isFullFlow);

        if (!mermaidSnip) {
            vscode.window.showErrorMessage("Failed to generate Mermaid diagram.");
            return;
        }

        // Determine the title based on whether it's full flow or a specific function
        const title = isFullFlow ? 'Full Flowchart' : `${functionName} Flowchart`;

        // Open WebView with the flowchart
        const panel = vscode.window.createWebviewPanel(
            'mermaidFlowchart',
            title, // Dynamic title based on context
            vscode.ViewColumn.One,
            { enableScripts: true }
        );

        // Set the WebView content
        panel.webview.html = getWebviewContent(mermaidSnip);
    });

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
                mermaid.initialize({ startOnLoad: true });
                window.addEventListener('load', () => {
                    mermaid.init();
                });
            </script>
            <style>
                body { font-family: Arial, sans-serif; }
                .mermaid { display: block; margin: 0 auto; }
                h1 { text-align: center; color: #007ACC; }
            </style>
        </head>
        <body>
        <center>
            <h1>Mermaid Flowchart</h1>
            <div class="mermaid">
                ${escapeHtml(mermaidSnip)}
            </div>
        </center>
        </body>
        </html>
    `;
}

function escapeHtml(text) {
    return text
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

module.exports = { activate };
