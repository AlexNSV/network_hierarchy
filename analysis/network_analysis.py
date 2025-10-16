import networkx as nx
import pandas as pd
import logging
from pyvis.network import Network
import pyvis
import pyreadr

def countCentrality(G: nx.Graph, country_list: list, centrality_type: str, prefix: str = ""):
    """A function to measure centrality in a network
    
    Parameters
    ------------
        G: networkx.Graph or networkx.DiGraph
            Network graph
        country_list: list
            List of all countries (or other agents) in the network
        centrality_type: str
            Method of centrality measurement (currently supports out-degree centrality and betweenness centrality)
        prefix: str
            Prefix to put in centrality, rank and status column names in the returned df. Default: empty string.
    Return
    -----------
        centrality_df : pd.DataFrame()
            DataFrame containing information about centrality, rank and status of all nodes in the network. 
    
    """
    if centrality_type == 'out-degree':
        centrality = nx.out_degree_centrality(G)
    elif centrality_type == 'out-degree-weighted':
        logging.info("analysing out-degree-weighted centrality")
    #    out_degree_centrality = {node: sum(weight for _, _, weight in G.out_edges(node, data='value')) for node in G.nodes()}
        centrality = {}
        for node in G.nodes():
            #return G, node
            out_strength = sum(G[node][neighbor]['weight'] for neighbor in G.successors(node))
            centrality[node] = out_strength
    elif centrality_type == 'betweenness':
        centrality = nx.betweenness_centrality(G)#, weight='value')
    elif centrality_type == 'laplacian':
        centrality = nx.laplacian_centrality(G, weight='value')
    elif centrality_type == 'pagerank':
        centrality = nx.pagerank(G.reverse(), weight='value')
    else:
        raise NotImplementedError(centrality_type)
    
    
    centrality_df = pd.DataFrame()
    
    if prefix != "":
        prefix = prefix + "_"
    for country in country_list:
        centrality_df.loc[country, f'{prefix}centrality'] = centrality[country]
    centrality_df[f'{prefix}rank'] = centrality_df.rank(ascending=False)
    centrality_df[f'{prefix}status'] = len(country_list) / centrality_df[f'{prefix}rank']
    return centrality_df

def get_networks(df_triple, countries_all, year_start, year_end, isDigraph=True, forceString=False, removeLessThanZero=True):
    """Getting global networks from df_triple

    Parameters
    ------------
        df_triple: pd.DataFrame()
            DataFrame containing information about dyadic relations (year*country*country). Index level names should be year, alter, ego 
        countries_all: set
            Set of all countries involved
        year_start: int
            First year to analyse, must be in dataframe
        year_end: int
            Last year to analyse, must be in dataframe
        forceString: bool
            Whether to forceString. Default is False. True setting True if getting error.
        
    Return
    -----------
        networks: dict
            Dictionary of networkx objects for each year
    """
    logging.info(f"Getting global networks for {year_start} - {year_end}")    
    networks = dict()
    for year in range(year_start, year_end + 1): # dont remember why +1
        logging.debug(f"Getting networks for {year}")
        networks[year] = get_network_from_year_df(df_triple.copy(),countries_all, year, forceString=forceString, isDigraph=isDigraph, removeLessThanZero=removeLessThanZero)  # remove forceString if error    
    logging.info("Done getting networks")
    return networks


def get_network_from_year_df(df_triple: pd.DataFrame(), countries: list(), year: int, 
                             ego_indexname: str = 'ego', alter_indexname: str = 'alter', 
                             value_indexname: str = 'value', isDigraph: bool = True, 
                             removeLessThanZero: bool = True, forceString: bool = True):
    """A function to get a Graph from a Multiindex Dataframe in the format [(year, ego, alter), edge_weight]
    
    Parameters
    ------------
        df_triple: pd.DataFrame
            Multiindex Dataframe in the format [(year, ego, alter), edge_weight]
        countries: list()
            List of countries in the DataFrane
        year: int
            Year to analyse
        ego_indexname: str
            Index level name for ego (outward node in directed graph, doesn't matter in undirected graphs)
            Default value: 'seller'
        alter_indexname: str
            Index level name for alter (inward node in directed graph, doesn't matter in undirected graphs)
            Default value: 'buyer'
        value_indexname: str
            Index level name for weight
            Default value: 'tivorder'
        isDigraph: bool 
            Whether to get a Directed (True) or Undirected (False) graph
            Default: True
        removeLessThanZero: bool 
            Whether to purge negative values
            Default: True
        forceString: bool 
            Whether to force interprete year as a string instead of an int
            Default: True
    Return
    -----------
        network : networkx.Graph() or networkx.DiGraph()
            Graph for the input data. 
    
    """
    #print(countries)
    #print(df_triple)
    if forceString:
        df_triple = df_triple.loc[str(year)]
    else:
        df_triple = df_triple.loc[year]
    df_triple = df_triple[df_triple.index.get_level_values(ego_indexname).isin(countries)]
    df_triple = df_triple[df_triple.index.get_level_values(alter_indexname).isin(countries)]
    df_triple = df_triple.reorder_levels([ego_indexname, alter_indexname])
    df_triple = df_triple[df_triple[value_indexname] > 0]
    #print(df_triple)
    year_tuples = list(df_triple.reset_index().itertuples(index=False, name=None))
    if isDigraph:
        network = nx.DiGraph()
    else:    
        network = nx.Graph()
    network.add_nodes_from(countries)
    network.add_weighted_edges_from(year_tuples)
    return network

def plotWithPyvis(G, notebook_plotting=True, improve_view=True, heading="", show_buttons=False, directed=True):

    #labels = make_label_dict(labels)
    
    net = Network(notebook=notebook_plotting, height='800px', width='1300px', heading=heading, directed=directed)
    net.from_nx(G)
    net.inherit_edge_colors = False
    #net.cdn_resources = 'remote'

    if show_buttons: net.show_buttons()
    
    #for i in range (0,len(labels)):
        #country = labels[net.nodes[i]['label']]
        #net.nodes[i]['label'] = country
        
    """if status[country] == 'superpower':
        net.nodes[i]['size'] = 50
        net.nodes[i]['mass'] = 50
    if status[country] == 'great_power':
        net.nodes[i]['size'] = 25
        net.nodes[i]['mass'] = 25
    if status[country] == 'middle_power':
        net.nodes[i]['size'] = 15
        net.nodes[i]['mass'] = 15
    if status[country] == 'small_power':
        net.nodes[i]['size'] = 7
        net.nodes[i]['mass'] = 7
    if status[country] == 'microstate':
        net.nodes[i]['size'] = 4
        net.nodes[i]['mass'] = 4"""
    #    pass
    #настройка толщины
    #for i in range (0,len(net.edges)):
        #country = labels[net.nodes[i]['label']]
        #net.edges[i]['value'] = net.edges[i]['weight'] / 10
    #    net.edges[i]['width'] = net.edges[i]['weight'] / 2 #for cosmetics
    net.set_options(
    """
    const options = {
      "nodes": {
        "font": {
          "size": 21
        }
      },
      "edges": {
        "color": {
          "opacity": 0.25
        }
      },
      "physics": {
        "barnesHut": {
          "gravitationalConstant": -8050,
          "centralGravity": 3.1,
          "springLength": 400
        },
      "minVelocity": 0.75
      }
    }
    """)
    #настройка всего
    #net.set_options(
    """
    var options = {
      "nodes": {
        "color": {
          "border": "rgba(233,172,190,1)"
        },
          "font": {
            "size": 40
          }
      },
      "edges": {
        "color": {
          "color": "rgba(93,226,185,0.33)",
          "inherit": false
        },
        "smooth": false
      },
      "physics": {
        "barnesHut": {
          "gravitationalConstant": -300,
          "springConstant": 0.035,
          "damping": 0.4
          }
      }
    }"""#)

 #   net.show_buttons(filter_=['physics'])
 #   net.show_buttons()
    return net