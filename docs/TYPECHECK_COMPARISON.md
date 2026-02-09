# Typechecker Comparison for bfb_delivery

This document compares different Python typecheckers tested on the bfb_delivery codebase and provides recommendations.

## Tools Tested

All tools available in the shared `typecheck` make target were tested:

1. **pytype** (existing) - Google's static type analyzer
2. **pyright** - Microsoft's static type checker  
3. **mypy** - The standard Python type checker from the Python community
4. **basedpyright** - A fork of pyright with additional features and stricter checking
5. **ty** - Fast typechecker built in Rust by Astral
6. **pyrefly** - Experimental typechecker

## Test Results Summary

### Error Counts (on src/ only, with minimal config + type ignores)

| Tool | Errors | Warnings | Config Required | Status |
|------|--------|----------|-----------------|--------|
| pytype | 0 | 0 | None | ✅ PASS |
| mypy | 0 | 0 | Minimal (ignore_missing_imports) + 20 type: ignore | ✅ PASS |
| ty | 63 | 0 | None | ❌ FAIL |
| pyrefly | 82 | 22 suppressed | None | ❌ FAIL |
| pyright | 104 | 0 | None | ❌ FAIL |
| basedpyright | 129 | 894 | None | ❌ FAIL |

## Detailed Analysis

### pytype (Currently Used)
**Pros:**
- Already integrated and passing with zero errors
- Well-suited for codebases using libraries like pandas, openpyxl
- More lenient with dynamic Python patterns
- Good balance of strictness vs. usability

**Cons:**
- Does not support Python 3.13+ (currently disabled for 3.13+)
- Slower than some alternatives
- Less strict than mypy/pyright in some cases

**Errors:** None

**Recommendation:** Continue using as one of the typecheckers.

### mypy (Recommended)
**Pros:**
- Standard in the Python community
- Good documentation and wide adoption
- Can be configured for varying strictness levels
- **Now passing with minimal configuration**

**Cons:**
- Required 20 `# type: ignore` comments to pass
- Most ignores are for default argument types from dict lookups
- Some legitimate typing complexity with pandera DataFrames

**Configuration Added:**
```toml
[tool.mypy]
ignore_missing_imports = true
```

**Errors:** 0 (after adding type ignores)

**Recommendation:** **Enable mypy** - it provides valuable additional type checking with minimal overhead.

### ty
**Pros:**
- Very fast (built in Rust)
- Modern tool from Astral (makers of ruff, uv)
- Active development

**Cons:**
- 63 errors on this codebase
- Many "unresolved-import" errors for installed packages
- Environment detection issues
- Less mature than mypy/pyright

**Errors:** 63 diagnostics (primarily import resolution)

**Recommendation:** Skip for now - too many unresolved import issues despite packages being installed.

### pyrefly
**Pros:**
- Experimental features
- Good error messages

**Cons:**
- 82 errors on this codebase
- Many pandera Config override errors
- numpy.bool vs bool incompatibilities
- Experimental/less mature

**Errors:** 82 (22 suppressed)

**Recommendation:** Skip for now - experimental status and high error count.

### pyright
**Pros:**
- Fast performance
- Integrated with VS Code
- Good type inference
- Active development by Microsoft

**Cons:**
- 104 errors on this codebase
- Very strict about pandas/DataFrame operations
- Many errors with Series vs DataFrame type inference

**Errors:** 104

**Recommendation:** Skip for now - too many errors for practical use without extensive refactoring.

### basedpyright
**Pros:**
- Even more features than pyright
- Stricter checking can catch more bugs

**Cons:**
- 129 errors + 894 warnings on this codebase
- Too strict for this codebase without extensive refactoring
- Many warnings about "Unknown" types from pandas operations

**Errors:** 129 errors + 894 warnings

**Recommendation:** Skip for now - too strict for practical use.

## Common Error Patterns

Errors that required `# type: ignore` in mypy:

1. **Dict default arguments** (15 occurrences) - Default values from dicts like `Defaults.BUILD_ROUTES_FROM_CHUNKED["output_dir"]` have union types
2. **Pandera DataFrame typing** (3 occurrences) - Type narrowing issues with schema-validated DataFrames
3. **Dynamic indexing** (2 occurrences) - Object types from list comprehensions

Remaining errors in other tools:

1. **Import resolution** (ty) - Cannot find installed packages
2. **Pandera Config overrides** (pyrefly, pyright, basedpyright) - pandera inheritance patterns
3. **numpy.bool vs bool** (pyrefly, basedpyright) - Return type incompatibilities
4. **openpyxl union types** (pyright, basedpyright) - Cell | MergedCell narrowing

## Runtime Comparison

All tools run quickly:
- pytype: ~90 seconds (full analysis with caching)
- mypy: ~5 seconds
- pyright: ~3 seconds
- basedpyright: ~3 seconds
- ty: ~2 seconds (fastest)
- pyrefly: ~4 seconds

## Final Recommendation

**Enable both pytype AND mypy:**

1. **pytype** - Keep enabled (already passing)
   - Provides good baseline type checking
   - Well-suited for pandas/numpy code
   
2. **mypy** - **Enable** (now passing with minimal config)
   - Industry standard
   - Complementary to pytype
   - Required only minimal configuration and 20 type: ignore comments
   - Provides additional coverage and catches different error patterns

3. **Skip** ty, pyrefly, pyright, basedpyright for now
   - Too many errors (63-129 errors each)
   - Would require substantial code refactoring or extensive type: ignore usage
   - Can revisit when tools mature or for Python 3.13+ migration

### Benefits of Running Both pytype and mypy

- **Defense in depth**: Different tools catch different issues
- **Low overhead**: Both pass with minimal configuration
- **Future-proofing**: When pytype drops Python 3.12 support, mypy is already integrated
- **Standard compliance**: mypy ensures code follows Python typing standards

## Configuration Files

- `pyproject.toml` - Contains minimal mypy configuration:
  ```toml
  [tool.mypy]
  ignore_missing_imports = true
  ```

- No configuration files created for other tools (following "minimal config" principle)
