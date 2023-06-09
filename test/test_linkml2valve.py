import valve_linkml.linkml2valve

# Every LinkM class name should be in a table column in the Table table
# Every LinkML slot name should be in a column column in the Column table

generate_data = True
valve_linkml.linkml2valve.map_schema("test/linkml2valve/schema/personinfo.yaml", generate_data)