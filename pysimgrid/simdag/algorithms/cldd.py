
import networkx
import numpy
import random
from pysimgrid import simdag
from .. import scheduler
from ... import cscheduling
from ..scheduler import StaticScheduler
from collections import deque
from collections import OrderedDict
import operator
from operator import itemgetter

class CLDD(scheduler.StaticScheduler):

  
    
    
  def get_schedule(self, simulation):

    graph = simulation.get_task_graph()
    platform_model = cscheduling.PlatformModel(simulation)
    
    no_of_tasks = 0
    summ=0
    no_of_children=0
    max_parent_cost=0
    cost = {task1: task1.amount for task1 in graph}
    schedule = {host: [] for host in simulation.hosts}
    hosts_count = len(simulation.hosts)
    height =0;
    level={}
    max_level = {}
    early = {}
    tasks_count =0
    current_min = cscheduling.MinSelector()
    j=-1
    i=-1
    level_list = []
    ho =[]
    state = cscheduling.SchedulerState(simulation)
    for idx, task in enumerate(networkx.topological_sort(graph)):
      tasks_count = tasks_count+1

    for idx, task in enumerate(networkx.topological_sort(graph)):
      l=-1
      level[task] = self.get_level(self,task,simulation,l)
      max_level[task]= max(level.values())
    
      
    for host, timesheet in state.timetable.items():
      for idx, task in enumerate(networkx.topological_sort(graph)):
        early[task] = platform_model.est(host,graph.pred[task], state)
        eet = platform_model.eet(task, host)
        pos, start, finish = cscheduling.timesheet_insertion(timesheet, early[task], eet)
        current_min.update((finish, host.speed, host.name), (host, pos, start, finish))
        host, pos, start, finish = current_min.value
        state.update(task, host, pos, start, finish)
    while (j<(max(max_level.values())+1)):
      task_list = []
      for idx, task in enumerate(networkx.topological_sort(graph)):
        if max_level[task] == j :
          
          for child, edge in graph[task].items():
            summ = summ + child.amount
            no_of_children = no_of_children + 1
          if(no_of_children > 0 ) :
            mcc = summ/no_of_children
          else :
            mcc = 0
          for parent in graph.pred[task] :
            if not parent :
              max_parent_cost=0
            if parent.amount > max_parent_cost :
              max_parent_cost = parent.amount 
          task_list.append(task)

      rank = {task2: (cost.get(task2) + mcc + max_parent_cost) for task2 in task_list }
      sort_list = OrderedDict(sorted(rank.items(), key=itemgetter(1),reverse=True))
      print rank.values()
      h = []
      
 
           

      for task1 in sort_list.keys() :                                 
        min_eft = 100000000000000
        for p in simulation.hosts:
          ho.append(p)
        for pro in ho:
          
            #est = platform_model.est(pro,graph.pred[task1], state)
          eft = eft = 1+ task1.amount
   
          if eft<min_eft:
            min_eft = eft
            min_host = pro
          else :
            break
        
        schedule[min_host].append(task1)
        print task1.name, min_host.name
        ho.remove(min_host)
          
        
      j=j+1
     
    return schedule  

             
  @staticmethod
  def get_level(self,task,simulation,level):
    
    graph = simulation.get_task_graph()
    parent = graph.pred[task]
    
    for idx, task in enumerate(networkx.topological_sort(graph)):
      root = task
      break
    if not parent :
      return level
    for p in parent.keys():
      if p == root :
        return level+1
      else :    
        return self.get_level(self,p,simulation,level+1)
    
      


    
    
  
