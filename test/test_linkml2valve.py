import os
import valve_linkml.linkml2valve

# Every LinkM class name should be in a table column in the Table table
# Every LinkML slot name should be in a column column in the Column table

base_dir = "test/"
input_file = os.path.join(base_dir, "linkml_input", "personinfo.yaml")
output_dir = os.path.join(base_dir, "valve_output")
valve_linkml.linkml2valve.map_schema(input_file, output_dir, generate_data=True)