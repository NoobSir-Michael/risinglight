CREATE TABLE `test_table` (
  `id` bigint(20) NOT NULL,
  `dt` int(11) NOT NULL,
  `hour` int(11) NOT NULL,
  `user_id` bigint(20) NOT NULL,
  `action_id` bigint(20) NOT NULL,
  `sales` double,
  `volume` double,
  `pieces` bigint(20),
  `add_time` timestamp NOT NULL,
  `update_time` timestamp NOT NULL
)
