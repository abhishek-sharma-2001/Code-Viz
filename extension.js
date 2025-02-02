const vscode = require('vscode');
const fs = require('fs');
const path = require('path');
const { resolveDependencies, convertMapToMermaid } = require('./functionCallExtractor'); // Importing the functionCallExtractor module

function activate(context) {
    let disposable = vscode.commands.registerCommand('extension.showMermaidFlowchart', async () => {
        const editor = vscode.window.activeTextEditor;
        if (!editor) {
            vscode.window.showErrorMessage("No active editor found.");
            return;
        }

        const selection = editor.selection;
        const selectedText = editor.document.getText(selection);

        // Check if the selection is a function name
        if (!selectedText) {
            vscode.window.showErrorMessage("Please select a function name.");
            return;
        }

        // Get file path
        const filePath = editor.document.uri.fsPath;

        // Generate the Mermaid diagram based on the function calls
        console.log("filepath: " + filePath);
        console.log("selectedText: " + selectedText);
        const mermaidSnip = await generateMermaidDiagram(filePath, selectedText);

        if (!mermaidSnip) {
            vscode.window.showErrorMessage("Failed to generate Mermaid diagram.");
            return;
        }

        console.log("Generated Mermaid Diagram:", mermaidSnip);

        // Open a WebView to display the Mermaid flowchart
        const panel = vscode.window.createWebviewPanel(
            'mermaidFlowchart',
            'Mermaid Flowchart',
            vscode.ViewColumn.One,
            {
                enableScripts: true,
            }
        );

        // Set the WebView content
        panel.webview.html = getWebviewContent(mermaidSnip);
    });

    context.subscriptions.push(disposable);
}

async function generateMermaidDiagram(filePath, selectedFunction) {
    try {
        console.log("Generating Mermaid diagram for:", filePath);
        console.log("Selected function:", selectedFunction);

        // Resolve function calls for the given file
        const functionCalls = await resolveDependencies(filePath);
        
        console.log("Resolved function calls:", functionCalls);

        if (!functionCalls || functionCalls.size === 0) {
            console.log("No function calls found.");
            return "graph TD\n  No function calls found"; // Provide a fallback for no function calls
        }

        // Convert the function calls to Mermaid syntax
        console.log("functioncalls:", [...functionCalls]);
        const mermaidStr = convertMapToMermaid(functionCalls);

        console.log("Generated Mermaid Diagram String:", mermaidStr);

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
            <h1>Mermaid Flowchart</h1>
            <div class="mermaid">
                ${escapeHtml(mermaidSnip)}
            </div>
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