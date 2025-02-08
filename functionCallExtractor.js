const fs = require("fs");
const path = require("path");
const Parser = require("tree-sitter");
const Python = require("tree-sitter-python");

// Built-in Python functions to ignore
const BUILT_IN_FUNCTIONS = new Set([
  "print",
  "len",
  "range",
  "type",
  "int",
  "str",
  "float",
  "dict",
  "list",
  "set",
  "tuple",
  "max",
  "min",
  "sum",
  "abs",
  "sorted",
  "round",
  "open",
  "input",
  "map",
  "filter",
  "zip",
  "enumerate",
  "any",
  "all",
  "chr",
  "ord",
]);

async function extractFunctionCalls(filePath) {
  const parser = new Parser();
  parser.setLanguage(Python);

  const sourceCode = fs.readFileSync(filePath, "utf-8");
  const tree = parser.parse(sourceCode);

  const functionCalls = new Map();
  const definedFunctions = new Set();
  let currentFunction = null;

  const traverse = (node) => {
    // Check if this node is a function definition
    if (node.type === "function_definition") {
      currentFunction = node.child(1).text; // Get function name from node
      definedFunctions.add(currentFunction);
      functionCalls.set(currentFunction, []);
      console.log(`Found function definition: ${currentFunction}`);
    } else if (node.type === "call" && currentFunction) {
      // Check if this is a function call and it is not a built-in function
      const functionName = node.child(0)?.text;
      if (
        functionName &&
        !BUILT_IN_FUNCTIONS.has(functionName) &&
        !functionName.includes(".")
      ) {
        // Ensure that we don't track method calls like `result.add` or `logger.info`
        functionCalls.get(currentFunction).push(functionName);
        console.log(
          `Found function call: ${currentFunction} -> ${functionName}`
        );
      }
    }

    // Recursively traverse child nodes
    for (let i = 0; i < node.childCount; i++) {
      traverse(node.child(i));
    }
  };

  traverse(tree.rootNode);

  console.log("Captured function calls:", functionCalls);
  return functionCalls;
}

// Function to find all import statements in the file
function findImports(filePath) {
  const sourceCode = fs.readFileSync(filePath, "utf-8");
  const importRegex = /from\s+([a-zA-Z0-9_]+)\s+import\s+([a-zA-Z0-9_,\s]+)/g; // from src.db.mongo_util import ( awefweewte,twewerm,ewe) // await self.gsdgsdgedg()
  let imports = [];
  let match;
  while ((match = importRegex.exec(sourceCode)) !== null) {
    const moduleName = match[1];
    const functions = match[2].split(",").map((f) => f.trim());
    imports.push({ module: moduleName, functions });
  }
  return imports;
}

// Function to resolve dependencies dynamically by detecting imported modules
async function resolveDependencies(filePath, visitedFiles = new Set()) {
  if (visitedFiles.has(filePath)) return new Map(); // Avoid cycles

  visitedFiles.add(filePath);
  const functionCalls = await extractFunctionCalls(filePath);
  const imports = findImports(filePath);

  const allFunctionCalls = new Map([...functionCalls]);

  // Check if there are imports
  console.log(`Processing imports for ${filePath}:`, imports);

  for (let { module, functions } of imports) {
    const modulePath = path.resolve(path.dirname(filePath), `${module}.py`);
    if (fs.existsSync(modulePath)) {
      console.log(`Resolving imported functions from: ${modulePath}`);
      const importedFunctionCalls = await resolveDependencies(
        modulePath,
        visitedFiles
      );
      for (let [func, calls] of importedFunctionCalls) {
        allFunctionCalls.set(func, calls);
      }
    } else {
      // If the module doesn't exist, we just add the functions as placeholders
      for (let func of functions) {
        allFunctionCalls.set(func, []);
        console.log(
          `Module ${module} not found. Placeholder for function: ${func}`
        );
      }
    }
  }

  return allFunctionCalls;
}

function convertMapToMermaid(map) {
    if (!(map instanceof Map)) {
        throw new Error("Input must be a Map");
    }

    let mermaidStr = "graph TD\n";

    // Handling case for single function with no calls
    if (map.size === 1) {
        const [key, value] = [...map.entries()][0];
        // Add the function itself if there are no calls
        mermaidStr += `  ${key}\n`;
    } else {
        map.forEach((value, key) => {
            if (Array.isArray(value)) {
                const uniqueValues = [...new Set(value)]; // Remove duplicates
                uniqueValues.forEach((v) => {
                    mermaidStr += `  ${key} --> ${v}\n`;
                });
            } else {
                mermaidStr += `  ${key} --> ${value}\n`;
            }
        });
    }

    return mermaidStr;
}

async function extractFunctionFromPosition(filePath, position) {
    const parser = new Parser();
    parser.setLanguage(Python);

    const sourceCode = fs.readFileSync(filePath, "utf-8");
    const tree = parser.parse(sourceCode);

    // Traverse the tree to find the function at the position
    let currentFunction = null;

    const traverse = (node) => {
        if (node.type === "function_definition") {
            const startPosition = node.startPosition;
            const endPosition = node.endPosition;

            if (
                position.line >= startPosition.row &&
                position.line <= endPosition.row
            ) {
                currentFunction = node.child(1).text; // Function name
            }
        }

        // Recursively traverse child nodes
        for (let i = 0; i < node.childCount; i++) {
            traverse(node.child(i));
        }
    };

    traverse(tree.rootNode);
    return currentFunction;
}

// Example usage
// const startingFile = "v2/fileA.py"; // Change this if needed


// resolveDependencies(startingFile).then((allFunctionCalls) => {
//   const mermaidSnip = convertMapToMermaid(allFunctionCalls);
//   console.log(mermaidSnip)
// });

module.exports = { resolveDependencies, convertMapToMermaid, extractFunctionFromPosition };
