from collections import deque
import sqlparse
from typing import Set, List
from collections import defaultdict
import os, re


def extract_source_tables(ddl_query: str, all_valid_names: Set[str]):
    """
    Extracts table names from a SQL query that includes nested FROM statements.
    Returns a list of unique table names in the order they appear in the query.
    """
    sql_query = ddl_query.replace("`", "")
    table_names = set()
    regex = r'\b(?:FROM|JOIN|UNION)\b\s+([\w.]+)'
    matches = re.findall(regex, sql_query)
    for match in matches:
        table_name = match.lower()
        if ((all_valid_names and table_name in all_valid_names) or (not all_valid_names)) \
            and table_name not in table_names:
            table_names.add(table_name)
    return table_names

def unpack_view_db_name(full_view_name: str):
   parts = full_view_name.split(".")
   assert len(parts) == 2, f"{full_view_name} is not formatted correctly."
   return parts[0], parts[1]

def get_view_dependencies(metastore_view_dir: str, full_view_name: str, all_views: Set[str]):
    print(f"processing dependencies of {full_view_name}")
    db_name, vw = unpack_view_db_name(full_view_name)
    # ddl_query = spark.sql(f"show create table {view_name}").collect()[0][0]
    ddl_full_path = os.path.join(metastore_view_dir, db_name, vw)
    dep_set = set()
    with open(ddl_full_path, "r") as f:
        ddl_query = f.read()
        identifiers = extract_source_tables(ddl_query, all_views)
        for token in identifiers:
            if full_view_name.lower() in token.lower():
                continue
            dep_set.add(token)
        print(f"dependencies: {dep_set}")
    return dep_set

def create_dependency_graph(metastore_view_dir: str, all_views: Set[str]):
    view_parents_dct = dict()
    for view_name in all_views:
        dep_views = get_view_dependencies(metastore_view_dir, view_name, all_views)
        view_parents_dct[view_name] = dep_views
    return view_parents_dct

def sort_views_topology(view_parents_dct):
    view_children_dct = defaultdict(set)
    q = deque([])
    for view, parents in view_parents_dct.items():
        for pview in parents:
            view_children_dct[pview].add(view)
        if not parents:
            q.append(view)
    sorted_views = []
    while q:
        cur_view = q.popleft()
        sorted_views.append(cur_view)
        for child_view in view_children_dct[cur_view]:
            view_parents_dct[child_view].remove(cur_view)
            if not view_parents_dct[child_view]:
                q.append(child_view)
    return sorted_views