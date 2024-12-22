# Contributing to reference_package

  Thank you for your interest in contributing to reference_package! I'm sure you have a good idea for how to improve this package or documentation etc., and I look forward to reviewing your proposal. That said, please comply with the following procedures and guidelines before opening a pull request (PR):
  - Read our [Code of Conduct](./CODE_OF_CONDUCT.md) to keep our community approachable and respectable.
  - Please open an issue first. Only open a PR if you've received explicit approval to do so from an admin in response to an issue. The reason is primarily that opening a PR will trigger a workflow run, which costs money, and the repo owner is not made of money. Also, this gives us a chance to work with you to refine your request, check if we aren't already working on a solution, etc.
  - Please each issue/PR limited to a focused scope. This is more important for PRs than for issues. We'd rather hear from you than not even if you're not yet able to clearly isolate an issue, but we don't want to review sprawling PRs. See rule number 1.
  - For bug reports:
    - Do your best to create the minimum reproducible example of a bug.
    - Include details about your environment (OS, Python version, etc.).
    - Include the full stack trace and failing call.
  - For PRs, whether bug fixes or new features, etc.:
    - Add test coverage for every piece of expected behavior. Did you fix a bug? Make a test that reproduces the bug and proves you've fixed it. Adding a new feature? Prove it does what it is supposed to do by creating a test and/or a set of tests.
    - Functions/methods should do a discrete thing and/or wrap a set of function calls that do discrete things. As a rule of thumb, limit functions to <= 50 lines.
    - Functions should be "properly structured." That essentially means that there is only one exit to a function, a single return statement. This is somewhat a matter of the repo owner's taste, but it is also an old-school standard that has stood the test of time, and while there are legitimate cases for relaxing this standard, adhering to it (along with other coding standards) at least does no harm, and improves legibility for those accustomed to it. As such, please adhere to it.
    - Avoid externalities like the plague:
      - Functions should only modify the objects limited to the function scope.
      - Use `typing.Final` to typehint constants. Constants shoud be treated as constants.
    - Function/method docstrings follow Google Python styleguide (or close to it for other languages): https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings
    - Copy edit your prose (spelling, grammar, fact check, link check, etc.). This includes comments and error/warning/logging messages, not just docstrings and formal documentation. Code is for humans.
