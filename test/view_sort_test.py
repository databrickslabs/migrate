import unittest
from unittest.mock import MagicMock
from dbclient import HiveClient
from dbclient.test.TestUtils import TEST_CONFIG
from io import StringIO
from dbclient.common.ViewSort import sort_views_topology, get_view_dependencies
from unittest import mock

class TestViews(unittest.TestCase):
    def test_sort_views_topology(self):
        view_parents_graph = {
                "view1": {"view2", "view3"},
                "view3": {"view4"},
                "view2": {},
                "view4": {"view5", "view6"},
                "view5": {},
                "view6": {},
                "view7": {}
        }
        views = sort_views_topology(view_parents_graph)
        assert views.index("view1") > views.index("view2") and views.index("view1") > views.index("view3") \
            and views.index("view3") > views.index("view4") \
            and views.index("view4") > views.index("view5") and views.index("view4") > views.index("view6")
        
    def test_get_view_dependencies(self):
        view_ddl = """
        CREATE VIEW `default`.`test_view` (
  first_name,
  middle_name,
  last_name,
  relationship_type_cd,
  receipt_number)
TBLPROPERTIES (
  'transient_lastDdlTime' = '1674499157')
AS SELECT
         p.first_name AS first_name,
         p.middle_name AS middle_name,
         p.last_name AS last_name,
         pc.role_id AS relationship_type_cd,
         pc.receipt_number AS receipt_number
     FROM `db1`.`persons` pc 
     JOIN `db2`.`person` p
         ON pc.person_id = p.person_id
         AND pc.svr_ctr_cd = p.svr_ctr_cd  
     WHERE 
         pc.role_id = 11
         AND (p.first_name is not null or p.middle_name is not null or p.first_name is not null )
        """
        mock_open = mock.mock_open(read_data=view_ddl)
        with mock.patch("builtins.open", mock_open):
            deps = get_view_dependencies("/tmp/metastore_view", "default.test_view", {})
            assert deps == set(["db1.persons", "db2.person"])


    def test_get_view_deps_nested(self):
        view_ddl = """
        CREATE VIEW test.view1 (
  step_rank,
  same_step_instance,
  id,
  t_cd)
AS SELECT ROW_NUMBER() OVER (PARTITION BYID ORDER BY st_cd_start_date) AS step_rank,
	ROW_NUMBER() OVER (PARTITION BY id, st_cd ORDER BY st_cd_start_date) AS same_step_instance,
	id, 
	st_cd,
	st_cd_start_date,
	st_cd_end_date,
	datediff(st_cd_end_date, st_cd_start_date) AS step_duration
    FROM (
	SELECT id, st_cd, st_cd_start_date
	FROM (
		SELECT id, NVL(st_cd, 'Null') AS st_cd
                		FROM test.view2 ch
	) aa
	WHERE Is_Boundry = 1) bb
WHERE st_cd_start_date IS NOT NULL
        """
        mock_open = mock.mock_open(read_data=view_ddl)
        with mock.patch("builtins.open", mock_open):
            deps = get_view_dependencies("/tmp/metastore_view", "tdss.case_actn_hist_st_cd_instances", {})
            assert len(deps) == 1 and next(iter(deps)) == "test.view2"