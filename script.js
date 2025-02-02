// script.js in the root directory
function runCode(functionName) {
    return 'Running JavaScript for function: ' + functionName;
}

function copyTextToClipboard(text) {
    navigator.clipboard.writeText(text);
    alert("Text copied to clipboard!");
}
