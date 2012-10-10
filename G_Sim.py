from __future__ import division
__metaclass__=type
import random
import csv
import multiprocessing
import Queue
import os


# Defines classes "Station" and "Players"
class Player:
	def __init__(self,arrival_date,player_ID):
		self.arrival_date=arrival_date
		self.player_ID=player_ID
		self.decision_date=[arrival_date]
		self.location=0
		self.in_sys=True
		self.skip=[]
		self.server_used=[]
		self.service_start_date=[]
		self.service_time=[]
		self.service_end_time=[]
		self.wait=[]
		self.cost=[]
		self.done=0
		self.total_cost=0
		self.expected_cost=[]
	
	def make_decision(self,beta,queue_current):
		if beta>len(queue_current):
			self.skip.append("False")
		else:
			self.skip.append("True")

	def find_player(self,t):
		b=0
		for e in self.decision_date:
			if e<=t:
				b+=1
		self.location=b-1
		

class Station:
	def __init__(self,No_servers,mu,station_ID,skip_cost):
		self.station_ID=station_ID
		self.beta=skip_cost
		self.service_rate=mu
		self.earliest=0
		self.servers_end=[]
		self.server_utilisation=[]
		self.queue=[]
		self.expected_cost_value=0
		self.skip_count=0
		
		h=0
		while h<No_servers:
			self.servers_end.append(0)
			self.server_utilisation.append(0)
			h+=1
			
	def expected_cost(self,No_servers,mu,queue_length):	
		
		if queue_length<len(self.servers_end):
			self.expected_cost_value=(1/mu)
			
		else:
			self.expected_cost_value=(len(self.queue)-1)/(No_servers*mu)
		
		
	def join_queue(self,decision_maker_ID):
		self.queue.append(decision_maker_ID)
		
	def queue_cleanup(self,Players,current_queue,k,t):
		h=0
		if current_queue==[]:
			return
		for e in current_queue:
			if Players[e].service_end_time[k]<t:
				current_queue.pop(current_queue.index(e))
			h+=1
		self.queue=current_queue

def find_decision_maker(Players,poss_decision_maker,t):

	decision_maker_ID=poss_decision_maker[0]
	for e in poss_decision_maker[1:]:
		if Players[decision_maker_ID].decision_date[-1]>=Players[e].decision_date[-1]:
			decision_maker_ID=e
	return decision_maker_ID

	
def sys_exit(n,Players,t):	
	for e in Players:
		if e.in_sys==True and e.location==n and e.decision_date[-1]<=t:
			e.in_sys=False
			
#Takes a linear map of the policy in terms of queue length
def policy_conversion(mu,No_servers,skip_cost):
	M_policy=[]
	k=0
	while k<len(mu):
		M_policy.append(int(mu[k]*No_servers[k]*skip_cost[k]))
		k+=1
	return M_policy

#Defining our sampling distributions		
def inter_arrival(lmbda):
	return random.expovariate(lmbda)
	
def service_dist(mu,location):
	return random.expovariate(mu[location])

def G_Sim(lmbda=False,mu=False,No_servers=False,skip_cost=False,Simulation_Time=False,Policy=False,warm_period=0,Print_Option=True):
	
	Policy_conversion=0
	#If not parameters input, prompt
	if not lmbda:
		lmbda=input('Inter arrival rate: ')
	if not mu:
		mu=input('Service rate: ')
	if not Simulation_Time:
		Simulation_Time=input('Total simulation time: ')
	if not No_servers:
		No_servers=input('Number of Servers: ')
	if not Policy:
		Policy=skip_cost
		Policy_conversion=1
	
	#Initialise the variables and lists
	t=0
	arrival_date=inter_arrival(lmbda)
	Players=[]
	Stations=[]
	poss_decision_maker=[]
	player_ID=0
	skip_count=0
	last_service=0
	
	#Validates input
	if not len(mu)==len(No_servers)==len(skip_cost):
		print "List lengths do not match"
		return
	
	#Creates Stations based on vector length
	k=0
	n=len(mu)
	while k<n:
		Stations.append(Station(No_servers[k],mu[k],k,skip_cost[k]))
		k+=1
		
	#Converts policy into terms of queue length
	if Policy_conversion==1:
		N_policy = policy_conversion(mu,No_servers,skip_cost)
	else:
		N_policy = Policy
	
	#Simulation model
	while t<Simulation_Time:
		
		#Creating a new player if this is the models first run through, or when t is at the previous players arrival date
		if len(Players)==0:
			Players.append(Player(arrival_date,len(Players)))
			poss_decision_maker.append(Players[-1].player_ID)
		else:
			if len(Players[-1].decision_date)>1 or len(poss_decision_maker)==0:
				arrival_date+=inter_arrival(lmbda)
				Players.append(Player(arrival_date,len(Players)))
				poss_decision_maker.append(Players[-1].player_ID)
			
		#Increments the clock
		t=Players[poss_decision_maker[0]].decision_date[-1]
		for e in poss_decision_maker[1:]:
			t=min(t,Players[e].decision_date[-1])
		
		#Updates location for all players still in the system
		for e in Players:
			if e.in_sys==True:
				e.find_player(t)
				
		#checks if the players are still in the system
		sys_exit(n,Players,t)
		
		#chooses the current decision maker
		decision_maker_ID=find_decision_maker(Players,poss_decision_maker,t)

		#Cleans up all queues
		k=0
		while k<n:
			Stations[k].queue_cleanup(Players,Stations[k].queue,k,t)
			k+=1
		
		#the player makes a decision whether or not to skip the queue
		Stations[Players[decision_maker_ID].location].expected_cost(len(Stations[Players[decision_maker_ID].location].servers_end),Stations[Players[decision_maker_ID].location].service_rate,len(Stations[Players[decision_maker_ID].location].queue))
		Players[decision_maker_ID].make_decision(N_policy[Players[decision_maker_ID].location],Stations[Players[decision_maker_ID].location].queue)
		Players[decision_maker_ID].expected_cost.append(Stations[Players[decision_maker_ID].location].expected_cost_value)

		#This is simulating the queue itself
		if Players[decision_maker_ID].skip[Players[decision_maker_ID].location]=="False":
			Stations[Players[decision_maker_ID].location].join_queue(decision_maker_ID)
			Stations[Players[decision_maker_ID].location].earliest=min(Stations[Players[decision_maker_ID].location].servers_end)
			Players[decision_maker_ID].server_used.append(Stations[Players[decision_maker_ID].location].servers_end.index(Stations[Players[decision_maker_ID].location].earliest))
			Players[decision_maker_ID].service_time.append(service_dist(mu,Players[decision_maker_ID].location))
			Players[decision_maker_ID].service_start_date.append(max(Players[decision_maker_ID].decision_date[Players[decision_maker_ID].location],Stations[Players[decision_maker_ID].location].earliest))
			Players[decision_maker_ID].wait.append(Players[decision_maker_ID].service_start_date[Players[decision_maker_ID].location]-Players[decision_maker_ID].decision_date[Players[decision_maker_ID].location])
			Players[decision_maker_ID].service_end_time.append(Players[decision_maker_ID].service_start_date[Players[decision_maker_ID].location]+Players[decision_maker_ID].service_time[Players[decision_maker_ID].location])
			Players[decision_maker_ID].cost.append(Players[decision_maker_ID].wait[Players[decision_maker_ID].location]+Players[decision_maker_ID].service_time[Players[decision_maker_ID].location])
			Players[decision_maker_ID].total_cost+=Players[decision_maker_ID].cost[Players[decision_maker_ID].location]
			Players[decision_maker_ID].decision_date.append(Players[decision_maker_ID].service_end_time[Players[decision_maker_ID].location])
			Stations[Players[decision_maker_ID].location].servers_end[Players[decision_maker_ID].server_used[-1]]=Players[decision_maker_ID].service_end_time[Players[decision_maker_ID].location]
			if Players[decision_maker_ID].service_end_time[-1]<Simulation_Time and Players[decision_maker_ID].arrival_date>warm_period:
				Stations[Players[decision_maker_ID].location].server_utilisation[Players[decision_maker_ID].server_used[-1]]+=Players[decision_maker_ID].service_time[-1]
			elif last_service==0 and Players[decision_maker_ID].arrival_date>warm_period:
				Stations[Players[decision_maker_ID].location].server_utilisation[Players[decision_maker_ID].server_used[-1]]+=(Simulation_Time-Players[decision_maker_ID].service_start_date[-1])
				last_service=1
			
		#This Part is for the players who skipped, filling attributes with N/A
		else:
			Players[decision_maker_ID].cost.append(skip_cost[Players[decision_maker_ID].location])
			Players[decision_maker_ID].server_used.append('N/A')
			Players[decision_maker_ID].service_time.append(0)
			Players[decision_maker_ID].service_start_date.append('N/A')
			Players[decision_maker_ID].wait.append(0)
			Players[decision_maker_ID].service_end_time.append('N/A')
			Players[decision_maker_ID].decision_date.append(Players[decision_maker_ID].decision_date[-1])
			Players[decision_maker_ID].total_cost+=Players[decision_maker_ID].cost[-1]
			Stations[Players[decision_maker_ID].location].skip_count+=1

		#Removes players whove made thier final decision.
		if len(Players[decision_maker_ID].decision_date)==n+1:
			poss_decision_maker.pop(poss_decision_maker.index(decision_maker_ID))
		
	#adds completed players to a new list
	completed_players=[]
	
	for e in Players:
		if e.location==n and e.arrival_date>warm_period:
			completed_players.append(e)

	#Summary Statistics
	if not len(completed_players)==0:

		k=0
		costs=[]
		Mean_cost=[]
		while k<n:
			costs.append([a.cost[k] for a in completed_players])
			Mean_cost.append(sum(costs[k])/len(costs[k]))
			k+=1
		Sys_total_cost=sum(Mean_cost)/n	
			
		if not Print_Option:
			for e in Stations:
				e.server_utilisation=[r/(t-warm_period) for r in e.server_utilisation]
				ave_Util=[]
			
			k=0
			while k<n:
				ave_Util.append(sum(Stations[k].server_utilisation)/len(Stations[k].server_utilisation))
				k+=1

			return 0,Sys_total_cost
		
		skip_count=[a.skip_count for a in Stations]
		total_skips=sum(skip_count)
		
		k=0
		waits=[]
		Mean_wait=[]
		while k<n:
			waits.append([a.wait[k] for a in completed_players])
			Mean_wait.append(sum(waits[k])/len(waits[k]))
			k+=1
		Sys_mean_wait=sum(Mean_wait)/n
		
		k=0
		Total_time=[]
		Mean_time=[]
		while k<n:
			Total_time.append([a.wait[k]+a.service_time[k] for a in completed_players])
			Mean_time.append(sum(Total_time[k])/len(Total_time[k]))
			k+=1
		Sys_total_times=sum(Mean_time)/n
		
		
		
		#Output summary stats
		print "Summary Statistics:"
		print ""
		print "Policy                    : ",N_policy
		print "Player Stats"
		print "Total Players             : ",len(completed_players)
		
		k=0
		while k<n:
			print "Mean Time for Station ",k," : ",Mean_time[k]
			k+=1
		print "Mean Time for System      : ",Sys_total_times
		
		k=0
		while k<n:
			print "Mean Wait for Station ",k," : ",Mean_wait[k]
			k+=1
		print "Mean Wait for System      : ",Sys_mean_wait
		
		k=0
		while k<n:
			print "Skips for Station",k,"      : ",Stations[k].skip_count
			k+=1
		print "Total Number of Skips     : ",total_skips
		k=0
		while k<n:
			print "Mean Cost for Station ",k," : ",Mean_cost[k]
			k+=1
		print "Mean Cost for the System  : ",Sys_total_cost
		print ""
		print "Station Stats"

		for e in Stations:
			e.server_utilisation=[r/(t-warm_period) for r in e.server_utilisation]
			h=0
			for r in e.server_utilisation:
				print "Server ",h,"at Station",e.station_ID," Utilisation: ", r
				h+=1
			print""
				
			
		print""
	
	else:
		if not Print_Option:
			ave_Util=[]
			k=0
			while k<n:
				ave_Util.append(0)
				k+=1

			Sys_total_cost=0

			return ave_Util,Sys_total_cost
		print "No Valid Players, Results Colection Period Too Small"
	
	#Csv writer,outputs data to seperate forms
	if input("Output data to csv (True/False)? "):

		#Outputs player data
		outfile=open('H_Q_S-Output-Player-(%s,%s,%s,%s,%s).csv' %(lmbda,mu,No_servers,skip_cost,Simulation_Time),'wb')
		output=csv.writer(outfile)

		#This is the title string
		outrow=[]
		outrow.append("Player ID")
		outrow.append("Arrival Date")
		h=0
		while h<n+1:
			string="Descision date %.0f" %(h)
			outrow.append(string)
			h+=1
		h=0
		while h<n:
			string="Server Used for Station  %.0f" %(h)
			outrow.append(string)
			h+=1
		h=0
		while h<n:
			string="Service Start Time for Station %.0f" %(h)
			outrow.append(string)
			h+=1
		h=0
		while h<n:
			string="Service Time for Station  %.0f" %(h)
			outrow.append(string)
			h+=1
		h=0
		while h<n:
			string="Service End Time for Station %.0f" %(h)
			outrow.append(string)
			h+=1
		h=0
		while h<n:
			string="Skip Decision for Station %.0f" %(h)
			outrow.append(string)
			h+=1
		h=0
		while h<n:
			string="Wait for Station %.0f" %(h)
			outrow.append(string)
			h+=1
		h=0
		while h<n:
			string="Cost for Station %.0f" %(h)
			outrow.append(string)
			h+=1
		h=0
		while h<n:
			string="Station %.0f Expected Cost" %(h)
			outrow.append(string)
			h+=1

		h=0
		while h<n:
			string="Decision at %s" %h 
			outrow.append(string)
			h+=1
		output.writerow(outrow)
		
		#This is the  player data
		k=0
		while k<len(Players):
			
			#Player data is being sent to CSV
			outrow=[]
			outrow.append(Players[k].player_ID)
			outrow.append(Players[k].arrival_date)
			h=0
			while h<n+1:
				if h<=Players[k].location:
					outrow.append(Players[k].decision_date[h])
				else:
					outrow.append('DNF')
				h+=1
			h=0
			while h<n:
				if h<=Players[k].location:
					outrow.append(Players[k].server_used[h])
				else:
					outrow.append('DNF')
				h+=1
			h=0
			while h<n:
				if h<=Players[k].location:
					outrow.append(Players[k].service_start_date[h])
				else:
					outrow.append('DNF')
				h+=1
			h=0
			while h<n:
				if h<=Players[k].location:
					outrow.append(Players[k].service_time[h])
				else:
					outrow.append('DNF')
				h+=1
			h=0
			while h<n:
				if h<=Players[k].location:
					outrow.append(Players[k].service_end_time[h])
				else:
					outrow.append('DNF')
				h+=1
			h=0
			while h<n:
				if h<=Players[k].location:
					outrow.append(Players[k].skip[h])
				else:
					outrow.append('DNF')
				h+=1
			h=0
			while h<n:
				if h<=Players[k].location:
					outrow.append(Players[k].wait[h])
				else:
					outrow.append('DNF')
				h+=1
			h=0
			while h<n:
				if h<=Players[k].location:
					outrow.append(Players[k].cost[h])
				else:
					outrow.append('DNF')
				h+=1
			
			h=0
			while h<n:
				if h<=Players[k].location:
					outrow.append(Players[k].expected_cost[h])
				else:
					outrow.append('DNF')
				h+=1

			h=0
			while h<n:
				if h<=Players[k].location:
					if Players[k].cost[h]<Stations[h].beta and Players[k].skip[h] == "True" or Players[k].cost[h]>Stations[h].beta and Players[k].skip[h] == "False":
						outrow.append("Incorrect")
					elif Players[k].cost[h]>=Stations[h].beta and Players[k].skip[h] == "True" or Players[k].cost[h]<=Stations[h].beta and Players[k].skip[h] == "False" :
						outrow.append("Correct")
				else:
					outrow.append('DNF')
				h+=1
			
			output.writerow(outrow)
			k+=1
		outfile.close()
		
		#Outputs Station data
		outfile=open('H_Q_S-Output-Station-(%s,%s,%s,%s,%s).csv' %(lmbda,mu,No_servers,skip_cost,Simulation_Time),'wb')
		output=csv.writer(outfile)
		
		outrow=[]
		outrow.append("Station ID")
		outrow.append("Station Skip Cost")
		outrow.append('Station Service Rate')
		outrow.append('No of Servers at Station')
		h=0
		k=0
		while k<n:
			while h<No_servers[k]:
				string="Server %.0f at Station %.0f Utilisation" %(h,k)
				outrow.append(string)
				h+=1
			k+=1
		output.writerow(outrow)
		
		k=0
		while k<n:
			outrow=[]
			outrow.append(Stations[k].station_ID)
			outrow.append(Stations[k].beta)
			outrow.append(Stations[k].service_rate)
			outrow.append(No_servers[k])
			h=0
			while h<No_servers[k]:
				outrow.append(Stations[k].server_utilisation[h])
				h+=1
			output.writerow(outrow)
			k+=1
			
		outfile.close()
	
	return
