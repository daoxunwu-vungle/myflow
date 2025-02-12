* Add workflow
** init workflow
create dir:
  `mkdir -p .github/workflows`
edit file:
  `nano .github/workflows/placeholder.yml`
  with content:
  ```
name: Placeholder Workflow

on:
  push:
    branches:
      - master
  pull_request:
    branches:
      - master

jobs:
  placeholder-job:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout Repository
        uses: actions/checkout@v4

      - name: Placeholder Step
        run: echo "This is a placeholder GitHub Actions workflow."
  ```
Push to github:
```
git add .github/workflows/placeholder.yml
git commit -m "Add placeholder GitHub Actions workflow"
git push origin master
```

* Add new section
