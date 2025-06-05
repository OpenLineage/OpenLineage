---
sidebar_position: 5
---

# Source Code Location Facet

The facet that indicates where the source code is located.

Example:

```json
{
    ...
    "job": {
        "facets": {
            "sourceCodeLocation": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeLocationJobFacet.json",
                "type": "git|svn",
                "url": "https://github.com/MarquezProject/marquez-airflow-quickstart/blob/693e35482bc2e526ced2b5f9f76ef83dec6ec691/dags/hello.py",
                "repoUrl": "git@github.com:{org}/{repo}.git or https://github.com/{org}/{repo}.git|svn://<your_ip>/<repository_name>",
                "path": "path/to/my/dags",
                "version": "git: the git sha | Svn: the revision number",
                "tag": "example",
                "branch": "main"
            }
        }
    }
    ...
}
```


The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/SourceCodeLocationJobFacet.json)