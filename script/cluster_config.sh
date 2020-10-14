#!/bin/bash
  
cluster_name="graph-10"
cluster_size=2
config_path="/home/ubuntu/path_config"


for ((i=0; i<$cluster_size; i++)); do
        echo $i
done


for ((i=0; i<$cluster_size; i++)); do
        if [ $i == 0 ]
        then
                source $config_path
        else
                node_name=""
                if [ $i -lt 10 ]
                then
                        node_name=${cluster_name}-node00$i
                elif [ $i -lt 100 ]
                then
                        node_name=${cluster_name}-node0$i
                else
                        node_name=${cluster_name}-node$i
                fi
                ssh $node_name "source $config_path && exit"
        fi
done
