from gurobipy import *
import numpy as np
from typing import Any, Dict, List, Optional
class optimization:
    def __init__(self,deployment: Dict[str, Any]):
        self.deployment=deployment
        self.workflow_functions=list(deployment['functions'].keys())
        self.providers=list(deployment['providers'].keys())
        self.num_functions = len(deployment['functions'])
        self.num_nodes_cloud = len(deployment['providers']) - 1
        self.num_nodes_tiny = len(deployment['providers']['tinyFaaS']['nodes'])
        self.num_nodes = self.num_nodes_cloud + self.num_nodes_tiny
        self.model=self.setup_model()


    def setup_model(self):
        model= Model("FaaS")
        C=np.zeros((self.num_functions,self.num_nodes))
        D=np.zeros((self.num_functions,self.num_nodes))
        L=np.zeros((self.num_nodes,self.num_nodes))
        for j in range(self.num_functions):
            ram = self.deployment['functions'][self.workflow_functions[j]]['ram']
            time = self.deployment['functions'][self.workflow_functions[j]]['time']
            requests = self.deployment['functions'][self.workflow_functions[j]]['requests']
            data_dependencies = self.deployment['functions'][self.workflow_functions[j]]['data_dependencies']
            for k in range(self.num_nodes):
                for key in data_dependencies.keys():
                    value = data_dependencies[key]
                    pricing_Storage_Transfer = self.deployment['providers'][key][
                        'pricing_Storage_Transfer']
                    if key!=self.providers[k-self.num_nodes_tiny+1]:
                        D[j,k]=D[j,k]+pricing_Storage_Transfer*value
                if k<self.num_nodes_tiny:
                    C[j,k]=0
                else:
                    pricing_RAM = self.deployment['providers'][self.providers[k-self.num_nodes_tiny+1]]['pricing_RAM']
                    pricing_StartRequest = self.deployment['providers'][self.providers[k-self.num_nodes_tiny+1]]['pricing_StartRequest']
                    C[j,k]=pricing_RAM*ram*time+pricing_StartRequest*requests

        for k in range(self.num_nodes):
            for m in range(self.num_nodes):
                if k!=m:
                    L[k,m]=self.deployment['providers'][self.providers[k]]['estimated_latency']

        #for j in range(self.num_functions):
        #    for k in range(self.num_nodes):
        #        if k>=self.num_nodes_tiny:
        #            C[j,k]=1
        #        if k!=1:
        #            D[j,k]=2
        #for k in range(self.num_nodes):
        #    for m in range(self.num_nodes):
        #        if k!=m:
        #            L[k,m]=1
        w_1=1
        w_2=1
        w_3=1
        P=model.addVars(self.num_functions,self.num_nodes,vtype=GRB.BINARY, name="P")
        obj=0
        for j in range(self.num_functions):
            for k in range(self.num_nodes):
              obj=obj+w_1*P[j,k]*C[j,k]+w_2*P[j,k]*D[j,k]
              for m in range(self.num_nodes):
                  if j<self.num_functions-1:
                    obj=obj+w_3*P[j,k]*P[j+1,m]*L[k,m]
        model.setObjective(obj,GRB.MINIMIZE)
        [model.addConstr(quicksum(P[j,k] for k in range(self.num_nodes))==1,"node_storage_constr"+str(j)) for j in range(self.num_functions)]
        model.setParam('OutputFlag', 1)
        model.update()
        return model
    def solve(self):
        self.model.optimize()
        P = np.zeros((self.num_functions, self.num_nodes))
        r = 0
        s = 0
        for v in self.model.getVars():
            P[s, r] = v.x
            r = r + 1
            if r == self.num_nodes:
                s = s + 1
                r = 0
        self.P=P
        print(P)
    def deploy_to_cloud(self,function_number: int,node_number: int):

        func_key=self.workflow_functions[function_number]
        if node_number==1:
            old_deploy=self.deployment['functions'][func_key]
            self.deployment['functions'][func_key]={
            "handler": "wrapper_aws.wrapper_aws",
            "requirements": "./functions/"+func_key+"/requirements.txt",
            "provider": "AWS",
            "method": "POST",
            "region": "us-east-1",
            "time": old_deploy['time'],
            "ram": old_deploy["ram"],
            "requests": old_deploy["requests"]
            },
        elif node_number==2:
            self.deployment['functions'][func_key]={
            "handler": "wrapper_gcp.wrapper_gcp",
            "requirements": "./functions/"+func_key+"/requirements.txt",
            "provider": "google",
            "method": "POST",
            "region": "us-central1"
            }
    def deploy_to_tinyfaas(self,function_number: int,node_number: int):
        func_key=self.workflow_functions[function_number]
        self.deployment['functions'][func_key]={
            "handler": "???",
            "requirements": "???",
            "provider": "tinyFaaS",
            "tinyFaaSOptions": {
                "env": "python3",
                "threads": 1,
                "source": "???",
                "deployTo": [
                    {
                        "name": "tf-node-"+str(node_number)
                    }
                ]
            }
        }

    def adjust_deployment_config(self):
        for j in range(self.num_functions):
            for k in range(self.num_nodes):
                if k<self.num_nodes_tiny and self.P[j,k]==1:
                    self.deploy_to_tinyfaas(j,k)
                elif self.P[j,k]==1:
                    self.deploy_to_cloud(j,k-self.num_nodes_tiny)

