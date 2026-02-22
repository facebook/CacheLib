# Contributing to CacheLib

We want to make contributing to this project as easy and transparent as possible. We appreciate all contributions, from bug fixes and documentation improvements to new features.

## Code of Conduct

All contributors are expected to follow our [Code of Conduct](CODE_OF_CONDUCT.md).

## Contributor License Agreement (CLA)

To accept your pull request, we need you to submit a CLA. You only need to do this once to work on any of Meta's open source projects.

Complete your CLA here: <https://code.facebook.com/cla>

## How to Contribute

We welcome contributions in many forms:

-   **Bug Reports**: If you find a bug, please file a detailed issue on GitHub.
-   **Feature Requests**: If you have an idea for a new feature, open an issue to discuss it.
-   **Pull Requests**: We actively welcome your pull requests for bug fixes, improvements, and new features.
-   **Documentation**: Improvements to our documentation are always welcome.

### Documentation Contributions

Our documentation is built with Docusaurus and is located in the `website/` directory. To contribute to the documentation:

1.  Edit the Markdown files in `website/docs/`.
2.  To preview your changes locally, run `cd website && npm install && npm start`.
3.  Submit a pull request with your changes.

### Pull Requests

1.  **Fork the repository** and create your branch from `main`.
2.  **Add tests** if you've added code that should be tested.
3.  **Update documentation** if you've changed APIs or added new features.
4.  **Ensure the test suite passes** by running `python3 ./build/fbcode_builder/getdeps.py --allow-system-packages test cachelib`.
5.  **Format your code** using `clang-format` with the provided `.clang-format` configuration.
6.  **Complete the CLA** if you haven't already.

We use a "rebase and merge" workflow. Please ensure your branch is up-to-date with `main` before submitting your PR.

#### Pull Request Template

When you open a pull request, please use the provided template and fill out the following sections:

-   **Summary**: A brief description of the changes.
-   **Test Plan**: How you tested your changes.
-   **Documentation**: A description of any documentation updates.

### Issues

We use GitHub issues to track public bugs. Please ensure your description is clear and has sufficient instructions to reproduce the issue.

For security bugs, please do not file a public issue. Instead, report it through Meta's [Whitehat Bug Bounty program](https://www.facebook.com/whitehat/).

## Code Style

We use `clang-format` to enforce a consistent code style. Please run `clang-format` on your changes before submitting a pull request. You can find the configuration in the `.clang-format` file in the root of the repository.

## License

By contributing to CacheLib, you agree that your contributions will be licensed under the Apache-2.0 License that covers the project.md project uses in this project.
