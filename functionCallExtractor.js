const fs = require("fs");
const path = require("path");
const Parser = require("tree-sitter");
const Python = require("tree-sitter-python");

// Set of built-in Python functions to ignore
const BUILT_IN_FUNCTIONS = new Set([
  "print", "len", "range", "type", "int", "str", "float", "dict",
  "list", "set", "tuple", "max", "min", "sum", "abs", "sorted",
  "round", "open", "input", "map", "filter", "zip", "enumerate",
  "any", "all", "chr", "ord",
]);

/**
 * Reads a file and returns its content as a string.
 * @param {string} filePath - The path to the file.
 * @returns {string} - File content.
 */
function readFileContent(filePath) {
  return fs.readFileSync(filePath, "utf-8");
}

/**
 * Extracts function calls from a given Python file.
 * @param {string} filePath - Path to the Python file.
 * @returns {Promise<Map>} - A map of function names and their calls.
 */
async function extractFunctionCalls(filePath) {
  console.log(`Analyzing function calls in: ${filePath}`);

  const parser = new Parser();
  parser.setLanguage(Python);

  const sourceCode = readFileContent(filePath);
  const tree = parser.parse(sourceCode);

  const functionCalls = new Map();
  const definedFunctions = new Set();
  let currentFunction = null;

  /**
   * Recursively traverses the AST (Abstract Syntax Tree) to find function calls.
   * @param {Object} node - The AST node.
   */
  function traverse(node) {
    if (node.type === "function_definition") {
      // Get function name
      currentFunction = node.child(1).text;
      definedFunctions.add(currentFunction);
      functionCalls.set(currentFunction, []);
      console.log(`Found function definition: ${currentFunction}`);
    } else if (node.type === "call" && currentFunction) {
      const functionName = node.child(0)?.text;

      if (
        functionName &&
        !BUILT_IN_FUNCTIONS.has(functionName) &&
        !functionName.includes(".") // Exclude method calls like obj.method()
      ) {
        functionCalls.get(currentFunction).push(functionName);
        console.log(`Detected function call: ${currentFunction} -> ${functionName}`);
      }
    }

    // Recursively traverse child nodes
    for (let i = 0; i < node.childCount; i++) {
      traverse(node.child(i));
    }
  }

  traverse(tree.rootNode);

  console.log("Function call map:", functionCalls);
  return functionCalls;
}

/**
 * Finds all import statements in a Python file.
 * @param {string} filePath - Path to the Python file.
 * @returns {Array} - List of imported modules and functions.
 */
function findImports(filePath) {
  console.log(`Extracting imports from: ${filePath}`);

  const sourceCode = readFileContent(filePath);
  const importRegex = /from\s+([a-zA-Z0-9_]+)\s+import\s+([a-zA-Z0-9_,\s]+)/g;

  let imports = [];
  let match;

  while ((match = importRegex.exec(sourceCode)) !== null) {
    const moduleName = match[1];
    const functions = match[2].split(",").map((f) => f.trim());
    imports.push({ module: moduleName, functions });
  }

  return imports;
}

/**
 * Resolves function dependencies by checking imports and function calls.
 * @param {string} filePath - Path to the starting Python file.
 * @param {Set} visitedFiles - Set of already visited files (to avoid cyclic dependencies).
 * @returns {Promise<Map>} - A map of function calls across all dependencies.
 */
async function resolveDependencies(filePath, visitedFiles = new Set()) {
  if (visitedFiles.has(filePath)) return new Map(); // Avoid infinite loops in cyclic imports

  visitedFiles.add(filePath);
  const functionCalls = await extractFunctionCalls(filePath);
  const imports = findImports(filePath);

  const allFunctionCalls = new Map([...functionCalls]);

  console.log(`Processing imports in ${filePath}:`, imports);

  for (let { module, functions } of imports) {
    const modulePath = path.resolve(path.dirname(filePath), `${module}.py`);

    if (fs.existsSync(modulePath)) {
      console.log(`Resolving dependencies for imported module: ${modulePath}`);
      const importedFunctionCalls = await resolveDependencies(modulePath, visitedFiles);

      for (let [func, calls] of importedFunctionCalls) {
        allFunctionCalls.set(func, calls);
      }
    } else {
      // If module file doesn't exist, add function placeholders
      for (let func of functions) {
        allFunctionCalls.set(func, []);
        console.log(`Module ${module} not found. Adding placeholder for function: ${func}`);
      }
    }
  }

  return allFunctionCalls;
}

/**
 * Generates a Graphviz DOT format representation of function call relationships.
 * @param {Map} allFunctionCalls - Function call map.
 * @returns {string} - Graphviz DOT formatted string.
 */
function generateGraphviz(allFunctionCalls) {
  let dotString = "digraph FunctionCalls {\n";
  dotString += '  node [shape=box, style="rounded, filled", fillcolor="lightblue"];\n';

  for (let [func, calls] of allFunctionCalls) {
    for (let calledFunc of calls) {
      dotString += `  "${func}" -> "${calledFunc}";\n`;
    }
  }

  dotString += "}";
  return dotString;
}

/**
 * Saves a Graphviz DOT file.
 * @param {string} dotContent - Graphviz formatted content.
 * @param {string} outputFilePath - File path to save the content.
 */
function saveGraphvizFile(dotContent, outputFilePath) {
  fs.writeFileSync(outputFilePath, dotContent, "utf-8");
  console.log(`Graphviz file saved at: ${outputFilePath}`);
}

/**
 * Converts function call map into Mermaid.js syntax.
 * @param {Map} map - Function call map.
 * @returns {string} - Mermaid.js formatted string.
 */
function convertMapToMermaid(map) {
  if (!(map instanceof Map)) {
    throw new Error("Input must be a Map");
  }

  let mermaidStr = "graph TD\n";

  if (map.size === 1) {
    const [key] = [...map.entries()][0];
    mermaidStr += `  ${key}\n`;
  } else {
    map.forEach((calls, func) => {
      const uniqueCalls = [...new Set(calls)];
      uniqueCalls.forEach((calledFunc) => {
        mermaidStr += `  ${func} --> ${calledFunc}\n`;
      });
    });
  }

  return mermaidStr;
}

/**
 * Extracts the function name at a given position in the file.
 * @param {string} filePath - Path to the Python file.
 * @param {Object} position - Line and column position.
 * @returns {Promise<string|null>} - The function name at the given position.
 */
async function extractFunctionFromPosition(filePath, position) {
  const parser = new Parser();
  parser.setLanguage(Python);

  const sourceCode = readFileContent(filePath);
  const tree = parser.parse(sourceCode);

  let currentFunction = null;

  function traverse(node) {
    if (node.type === "function_definition") {
      const start = node.startPosition;
      const end = node.endPosition;

      if (position.line >= start.row && position.line <= end.row) {
        currentFunction = node.child(1).text;
      }
    }

    for (let i = 0; i < node.childCount; i++) {
      traverse(node.child(i));
    }
  }

  traverse(tree.rootNode);
  return currentFunction;
}

module.exports = { resolveDependencies, convertMapToMermaid, extractFunctionFromPosition };
