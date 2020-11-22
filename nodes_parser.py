#%%
from datetime import datetime
import json
import csv

# file locations
trxn_in_file = "data/enriched_transactions.json"
edge_file = "data/"

#%% main


class bitcoin_parser:
    def __init__(self):
        self.output_dir = None
        self.in_edges_files = "in_edges.csv"
        self.node_file = "nodes.csv"
   
    def parse(self, input_file, output_dir):
        #reset counters
        self.output_dir = output_dir
        self.write_headers()
        cnt = 0
        with open(input_file) as fp:
            while(True):
                line = fp.readline()
                if not line: 
                    break
                cnt = cnt + 1
                l = json.loads(line)
                self.extract_features(l)
                self.extract_inputs(l)
        print(f'{cnt} rows processed')

    def write_headers(self):
        with open(self.output_dir + self.in_edges_files, 'w', newline='') as csvfile:
            csvfile.write(",".join(self.get_edge_header()))
        with open(self.output_dir + self.node_file, 'w', newline='') as csvfile:
            csvfile.write(",".join(self.get_node_header()))
         
    def extract_features(self, line):
        # transaction id
        features = [line['hash']]

        # size
        features.append(line['size'])
        features.append(line['virtual_size'])

        #version
        features.append(line['version'])

        #timestamps
        features.append(datetime.fromtimestamp(line['block_timestamp']/1000).strftime("%w"))

        # coinbasae
        features.append(line['is_coinbase'])

        #inputs
        features.append(len(line['inputs']))

        #outputs
        features.append(len(line['outputs']))

        with open(self.output_dir + self.node_file, 'a', newline='') as csvfile:
            csvfile.write(",".join(str(f) for f in features))

    def extract_inputs(self, line):
        for l in line['inputs']:
            # edge identifiers
            edge = [l['spent_transaction_hash']]
            edge.append(l['spent_output_index'])

            #in_trxn_hash
            edge.append(line['hash'])

            # script
            edge.append(len(l['script_asm']))

            # type
            edge.append(l['required_signatures'])
            edge.append(l['type'])
            edge.append(l['value'])

            with open(self.output_dir + self.in_edges_files, 'a', newline='') as csvfile:
                csvfile.write(",".join(str(e) for e in edge))


    def get_node_header(self):
        return ['node_id', 'size', 'virtual_size', 'version', 'weekday',
        'is_coinbase', 'input_cnt']

    def get_edge_header(self):
        return ['out_node', 'index', 'in_node', 'script_len', 'signatures', 'type', 'value']

#%%

bp = bitcoin_parser()
l = bp.parse(trxn_in_file, edge_file)
# %%
