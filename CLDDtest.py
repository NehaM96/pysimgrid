from pysimgrid import simdag
import pysimgrid.simdag.algorithms as algorithms
import logging
import pysimgrid.tools as tools

_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_LOG_FORMAT = "[%(name)s] [%(levelname)5s] [%(asctime)s] %(message)s"
logging.basicConfig(level=logging.DEBUG, format=_LOG_FORMAT, datefmt=_LOG_DATE_FORMAT)


with simdag.Simulation("test/data/pl_4hosts.xml", "test/data/basic_graph.dot") as simulation:
  scheduler = algorithms.CLDD(simulation)
  scheduler.run()
  graph = simulation.get_task_graph()
  #print(simulation.clock, scheduler.scheduler_time, scheduler.total_time)
  for t in simulation.tasks.sorted(lambda t: t.start_time):
        print(t.name, t.start_time, t.finish_time, t.hosts[0].name)
  
  print "TOTAL EXECUTION TIME :", sum([(t.finish_time - t.start_time) for t in simulation.tasks])
  print"TOTAL COMMUNICATION TIME :", sum([(t.finish_time - t.start_time) for t in simulation.connections])
    
