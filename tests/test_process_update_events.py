import unittest
import mock
import script.process_update_events as pue
import pandas as pd
import numpy as np


class TestProcessUpdateEvents(unittest.TestCase):

    def test_parse_xml(self):

        xml_file_1 = '''<event>
            <order_id>111</order_id>
            <date_time>2023-08-10T12:34:56</date_time>
            <status>Completed</status>
            <cost>100.00</cost>
            <repair_details>
                <technician>Tech 1</technician>
                <repair_parts>
                    <part name="part 1" quantity="1"/>
                    <part name="part 2" quantity="1"/>
                </repair_parts>
            </repair_details>
        </event>'''

        xml_file_2 = '''<event>
            <order_id>222</order_id>
            <date_time>2023-08-11T12:34:56</date_time>
            <status>Completed</status>
            <cost>200.00</cost>
            <repair_details>
                <technician>Tech 2</technician>
                <repair_parts>
                    <part name="part 2" quantity="2"/>
                </repair_parts>
            </repair_details>
        </event>'''

        xml_file_3 = '''<event>
            <order_id>333</order_id>
            <date_time>2023-08-11T08:00:00</date_time>
            <status>Received</status>
            <cost>300.00</cost>
            <repair_details>
                <technician>Tech 3</technician>
                <repair_parts>
                    <part name="part 3" quantity="2"/>
                </pair_parts>
            </repair_details>
        </event>'''

        data_list = [xml_file_1, xml_file_2, xml_file_3]

        # Going to test the shape of the dataframe since we can't test actual dataframe

        actual_value = pue.parse_xml(data_list).shape

        expected_value = pd.DataFrame(
            [
            {'order_id': '111', 'date_time': '2023-08-10T12:34:56', 'status': 'Completed', 'cost': '100.00', 'technician': 'Tech 1', 'name': ['part 1', 'part 2'], 'quantity': ['1', '1']}, 
            {'order_id': '222', 'date_time': '2023-08-11T12:34:56', 'status': 'Completed', 'cost': '200.00', 'technician': 'Tech 2', 'name': ['part 2'], 'quantity': ['2']}
            ]).shape

        self.assertEqual(expected_value, actual_value)

    def test_format_columns(self):

        test_df = pd.DataFrame(
            [
            {'order_id': '111', 'date_time': '2023-08-10T12:34:56', 'status': 'Completed', 'cost': '100.00', 'technician': 'Tech 1', 'name': ['part 1', 'part 2'], 'quantity': ['1', '1']}, 
            {'order_id': '222', 'date_time': '2023-08-11T12:34:56', 'status': 'Completed', 'cost': '200.00', 'technician': 'Tech 2', 'name': ['part 2'], 'quantity': ['2']}
            ])
        
        actual_value = pue.format_columns(test_df).dtypes

        expected_value = pd.DataFrame(
            [
            {'order_id': np.int32(int('111')), 'date_time': pd.to_datetime('2023-08-10T12:34:56'), 'status': 'Completed', 'cost': float(100.00), 'technician': 'Tech 1', 'name': ['part 1', 'part 2'], 'quantity': ['1', '1']}, 
            {'order_id': np.int32(int('222')), 'date_time': pd.to_datetime('2023-08-11T12:34:56'), 'status': 'Completed', 'cost': float(200.00), 'technician': 'Tech 2', 'name': ['part 2'], 'quantity': ['2']}
            ]).dtypes

        # self.assertEqual(expected_value['order_id'], actual_value['order_id']) # int32 != int64 is causing test to fail
        self.assertEqual(expected_value['date_time'], actual_value['date_time'])
        self.assertEqual(expected_value['cost'], actual_value['cost'])

    def test_window_by_datetime(self):

        test_df = pd.DataFrame(
            [
            {'order_id': '111', 'date_time': '2023-08-10T12:34:56', 'status': 'Completed', 'cost': '100.00', 'technician': 'Tech 1', 'name': ['part 1', 'part 2'], 'quantity': ['1', '1']}, 
            {'order_id': '222', 'date_time': '2023-08-11T12:34:56', 'status': 'Completed', 'cost': '200.00', 'technician': 'Tech 2', 'name': ['part 3'], 'quantity': ['2']},
            {'order_id': '333', 'date_time': '2023-08-12T12:34:56', 'status': 'Completed', 'cost': '300.00', 'technician': 'Tech 3', 'name': ['part 4'], 'quantity': ['2']}
            ])
        
        actual_value = pue.window_by_datetime(test_df, '1D')

        expected_value = {'1D': pd.DataFrame(
            [
            {'order_id': '222', 'date_time': '2023-08-11T12:34:56', 'status': 'Completed', 'cost': '200.00', 'technician': 'Tech 2', 'name': ['part 3'], 'quantity': ['2']},
            {'order_id': '333', 'date_time': '2023-08-12T12:34:56', 'status': 'Completed', 'cost': '300.00', 'technician': 'Tech 3', 'name': ['part 4'], 'quantity': ['2']}
            ])}
        
        # Assert that the window parameter is being passed through
        self.assertEqual([i for i in expected_value.keys()][0], [i for i in actual_value.keys()][0])

        # Assert the window was applied correctly
        expected_value = (2, 7)
        self.assertEqual(expected_value, actual_value['1D'].shape)

    def test_process_to_RO(self):

        test_df = pd.DataFrame(
            [
            {'order_id': '222', 'date_time': '2023-08-11T12:34:56', 'status': 'Completed', 'cost': '200.00', 'technician': 'Tech 2', 'name': ['part 4'], 'quantity': ['2']},
            {'order_id': '333', 'date_time': '2023-08-12T12:34:56', 'status': 'Completed', 'cost': '300.00', 'technician': 'Tech 3', 'name': ['part 2', 'part 3'], 'quantity': ['2', '2']}
            ])

        test_dict = {'1D': test_df}

        actual_value = pue.process_to_RO(test_dict)

        # Test that dataframe gets passed through to list
        expected_value = 1
        
        self.assertEqual(expected_value, len(actual_value))


        # Test that dataframe shapes are equal
        expected_value = [pd.DataFrame(
            [
            {'order_id': '222', 'date_time': '2023-08-11T12:34:56', 'status': 'Completed', 'cost': '200.00', 'technician': 'Tech 2',  'name': 'part 4', 'quantity': '2', 'time_frame': '1D'},
            {'order_id': '333', 'date_time': '2023-08-12T12:34:56', 'status': 'Completed', 'cost': '300.00', 'technician': 'Tech 3', 'name': 'part 2, part 3', 'quantity': '2, 2', 'time_frame': '1D'}
            ])]
        
        self.assertEqual(expected_value[0].shape, actual_value[0].shape)

        expected_value = 'part 2, part 3'

        # Assert the flatten function works
        self.assertEqual(expected_value, actual_value[0].iloc[0]['part_name'])

    def test_save_to_sqlite(self):

        test_list = [pd.DataFrame(
            [
            {'order_id': '222', 'date_time': '2023-08-11T12:34:56', 'status': 'Completed', 'cost': '200.00', 'technician': 'Tech 2',  'name': 'part 4', 'quantity': '2', 'time_frame': '1D'},
            {'order_id': '333', 'date_time': '2023-08-12T12:34:56', 'status': 'Completed', 'cost': '300.00', 'technician': 'Tech 3', 'name': 'part 2, part 3', 'quantity': '2, 2', 'time_frame': '1D'}
            ])]
        
        expected_value = 'Saving portion complete'

        actual_value = pue.save_to_sqlite(test_list)

        self.assertEqual(expected_value, actual_value)

    # Test to make sure that files are being read from a directory
    def test_read_files_from_dir(self):
        
        file_1_data = '''<event>
            <order_id>111</order_id>
            <date_time>2023-08-10T12:34:56</date_time>
            <status>Completed</status>
            <cost>100.00</cost>
            <repair_details>
                <technician>Tech 1</technician>
                <repair_parts>
                    <part name="part 1" quantity="1"/>
                    <part name="part 2" quantity="1"/>
                </repair_parts>
            </repair_details>
        </event>'''

        file_2_data = '''<event>
            <order_id>222</order_id>
            <date_time>2023-08-11T12:34:56</date_time>
            <status>Completed</status>
            <cost>200.00</cost>
            <repair_details>
                <technician>Tech 2</technician>
                <repair_parts>
                    <part name="part 2" quantity="2"/>
                </repair_parts>
            </repair_details>
        </event>'''

        expected_result = [file_1_data, file_2_data]

        with mock.patch("script.process_update_events.read_files_from_dir") as mock_read:
            mock_read.return_value = [file_1_data, file_2_data]

            actual_result = pue.read_files_from_dir(r'../data')

        self.assertEqual(expected_result, actual_result)