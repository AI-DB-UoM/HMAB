from database.qplan.index_use import PGIndexRead
from shared import helper_v2

po_read = {'Index Scan', 'Index Only Scan', 'Seq Scan'}


class PGReadQueryPlan:

    def __init__(self, xml_string):

        self.act_execution_time = 0
        self.act_planning_time = 0
        self.act_cpu_sum = 0
        self.non_clustered_index_usages = {}
        self.clustered_index_usages = {}

        self.root = helper_v2.strip_namespace(xml_string)
        stmt_simple = self.root.find('.//Query')
        self.act_planning_time = float(stmt_simple.find('Planning-Time').text)/1000
        self.act_execution_time = float(stmt_simple.find('Execution-Time').text)/1000

        self.rel_ops = self.root.findall('.//Plan')

        # Get the sum of sub tree cost for physical operations (assumption: sub tree cost is dominated by the physical
        # operations). Note we include the index insert as well (but not update or delete)
        self.total_sub_tree_cost = 0
        self.total_actual_elapsed = 0
        for rel_op in self.rel_ops:
            if rel_op.find('Node-Type').text in po_read:
                self.total_sub_tree_cost += float(rel_op.find('Total-Cost').text)
                self.total_actual_elapsed += float(rel_op.find('Actual-Total-Time').text)/1000

        node_id = 0
        for rel_op in self.rel_ops:
            if rel_op.find('Node-Type').text in po_read:
                # Getting information from rel-op level
                act_rows_output, act_cpu_max, act_elapsed_max, est_rows_output, index, table, index_kind = self.get_rel_op_info(rel_op)

                index_use = PGIndexRead(node_id, table, index, index_kind, act_elapsed_max, None, None, act_cpu_max,
                                      None, None, None, None, act_rows_output, est_rows_output)

                if rel_op.find('Node-Type').text in {'Index Scan', 'Index Only Scan'}:
                    self.non_clustered_index_usages[node_id] = index_use
                elif rel_op.find('Node-Type').text in {'Seq Scan'}:
                    self.clustered_index_usages[node_id] = index_use
            node_id += 1

        self.act_elapsed_max = self.act_execution_time + self.act_planning_time

    def __getitem__(self, *args):
        if isinstance(*args, str):
            return self.__dict__[str(*args)]
        keys = list(*args)
        return [self.__dict__[key] for key in keys]

    def get_rel_op_info(self, rel_op):
        """
        Get the basic attributes in the rel-op
        :param rel_op: xml element
        :return: tuple
        """

        act_rows_output = float(rel_op.find('Actual-Rows').text)
        act_cpu_max = float(rel_op.find('Total-Cost').text)
        act_elapsed_max = float(rel_op.find('Actual-Total-Time').text)/1000
        est_rows_output = float(rel_op.find('Plan-Rows').text)
        if rel_op.find('Index-Name') is not None:
            index = rel_op.find('Index-Name').text
        else:
            index = 'heap'
        table = rel_op.find('Relation-Name').text
        index_kind = rel_op.find('Node-Type').text

        return act_rows_output, act_cpu_max, act_elapsed_max, est_rows_output, index, table, index_kind


    @staticmethod
    def get_attr(element, attr_name, default=0):
        """
        Use this when you are not sure of the attribute availability
        :param element: xml tree element
        :param attr_name: String, Attribute Name
        :param default: Default value when the attribute is not available
        :return: String, attribute value
        """
        value = element.attrib.get(attr_name)
        value = value if value is not None else default
        return value





