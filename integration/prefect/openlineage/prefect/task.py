"""

# TODO - Allow passing callables to add additional facets to ouput data? Something like:

def some_facet(data) -> OutputFacet:
    ...


@LineageTask(output_facets=[some_facet])
def func(a):
    ...

"""


# from typing import Callable, List, Optional
#
# from prefect import Task
#
#
# class LineageTask(Task):
#     def __init__(self, output_facets: Optional[List[Callable]], **kwargs):
#         super().__init__(**kwargs)
#         self.output_facets = output_facets
#
#     def run(self) -> None:
#         result = super().run()
#         for facet_callbable in self.output_facets:
#             facet = facet_callbable(self, result)
#         return result
