import os
from bs4 import BeautifulSoup
import csv
import json
import re
import time as t
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import sqlite3
import pandas as pd
# import sys
# sys.stdout.reconfigure(encoding='utf-8')

class Scraper_immoweb():
	def __init__(self):
		self.links_sp=[]
		self.links_i1=[]
		self.links_i2=[]
		self.web_data=[]
		self.link_psd=0		

	def logmessage(self, text):		
		tfm = r"%d/%m/%Y %H:%M:%S" # time format for message
		print(datetime.now().strftime(tfm),"=", text, flush=True) 

	def cls_links_sp(self):
		self.links_sp.clear()	

	def get1_links_sp(self, pages, property_type):
		u = r'	https://www.immoweb.be/en/search/@property_type/for-sale?page=@page_number'	
		L = [	{ 	'np':i, 
					'prop_type':property_type,
		 			'link_sp': u.replace('@property_type', property_type).replace('@page_number',str(i)) 
				}  for i in range( 1 , pages+1 ) ] 
		self.links_sp.extend(L)
		self.logmessage(text=f'Search pages for {property_type} is generated')
		return L	

	def get3_response_sp(self, link): # return list of ind.links
		# time.sleep(random.uniform(0.1, 3.0)) #затримка з випадковою протяжністю		
		list_urls=[]
		url, np, prop_type = link['link_sp'], link['np'], link['prop_type']
		resp = requests.get(url, timeout=90)
		if resp.status_code==200:
			soup = BeautifulSoup(resp.text, 'html.parser')       # Parsing the search page ontent
			tags = soup.find_all("a", class_="card__title-link") # Finding all links on web page
			nlp = 1                                              # set fist link number on page			
			for t in tags:                                       # handeling links one-by-one 
				if t:
					href = t.get("href")                         # getting link
					# --save link to list, (nl=0 and ch=0 will proceed later)
					list_urls.append({ "nl":0, "ch":0, "nlp":nlp, "np":np, "prop_type":prop_type, "link":href})
					nlp +=1                                    	 # next link page number
		return list_urls                                         # result


	def get2_links_ind(self, max_workers):		#	def main():
		results_all=[]
		links = self.links_sp
		with ThreadPoolExecutor(max_workers) as p:	# creating workers
			results = p.map(self.get3_response_sp, links)
			for result in results:
				results_all.extend(result)	
		self.links_i1 = sorted(results_all, key=lambda x: (x['prop_type'], x['np'], x['nlp'])) # sorting results
		for i, row in enumerate(self.links_i1) : row['nl']=i+1 # nubering 'nl' 		
		self.logmessage(text=f'Individual links are collected = {len(self.links_i1)}')

	def get_response_i(self, link):
		ddict = {}				  
		# time.sleep(random.uniform(0.1, 10)) # random delay before the request  
		resp = requests.get(link['link'], timeout=300)
		text = resp.text	
		# Define the pattern to match the script content
		pattern = re.compile(r'<script type="text/javascript">(.*?)</script>', re.DOTALL)	
		matches = pattern.findall(text.strip()) # Use findall to get all matches in the string
		jtext1 = matches[0].strip()
		jtext  = jtext1[19:][:-1]
		# print(jtext[3:5])
		if jtext[3:5] != 'id': 
			return [] # if "id" not exist on the right place, stop running
		row_data = json.loads(jtext) # convert row text to python dict <class 'dict'>
		
		if self.link_psd%1000==0 and self.link_psd>0:
			# Printing only number because it has a problem with ThreadPoolExecutor
			print(self.link_psd)	
		self.link_psd+=1
	

	# ------ INDIVIDUAL ADS
		ddict["nl"]  = link["nl"]
		ddict["ch"]  = link["ch"]
		ddict["nlp"] = link["nlp"]
		ddict["np"]  = link["np"]
		ddict["prop_type"]  = link["prop_type"]
		ddict["link"]= link["link"]

		try: # --- id
			t = row_data["id"]            
		except (KeyError, TypeError):
			t = None
		ddict["id"] = t

		try: # --- Locality_Name       
			t = row_data["property"]["location"]["locality"]           
		except (KeyError, TypeError):
			t = None
		ddict["Locality_Name"] = t

		try: # --- Postal_code
			t = row_data["property"]["location"]["postalCode"]           
		except (KeyError, TypeError):
			t = None
		ddict["Postal_code"] = t

		try: # --- Type_of_property
			t = row_data["property"]["type"]            
		except (KeyError, TypeError):
			t = None
		ddict["Type_of_property"] = t

		try: # --- Subtype_of_property
			t   = row_data["property"]["subtype"]            
		except (KeyError, TypeError):
			t = None
		ddict["Subtype_of_property"] = t

		try: # --- Price
			t = row_data["transaction"]["sale"]["price"]           
		except (KeyError, TypeError):
			t = None
		ddict["Price"] = t

		try: # --- Type_of_sale
			t  = row_data["transaction"]["type"]            
		except (KeyError, TypeError):
			t= None
		ddict["Type_of_sale"] = t

		try: # --- Number_of_rooms
			t = row_data["property"]["bedroomCount"]            
		except (KeyError, TypeError):
			t = None
		ddict["Number_of_rooms"]=t

		try: # --- Living_Area
			t = row_data["property"]["netHabitableSurface"]            
		except (KeyError, TypeError):
			t = None
		ddict["Living_Area"]=t

		try: # --- Equipped_kitchen
			t = row_data["property"]["kitchen"]["type"]            
		except (KeyError, TypeError):   
			t = None
		ddict["Equipped_kitchen"] = t        
		
		try: # --- Furnished   
			t = str(row_data["transaction"]["sale"]["isFurnished"])
		except (KeyError, TypeError):   
			t = None
		ddict["Furnished"] = t     
		
		try: # --- Open_fire   
			t = str(row_data["property"]["fireplaceExists"])
		except (KeyError, TypeError):   
			t = None
		ddict["Open_fire"] = t
		
		try: # --- Terrace_YN   
			t = str(row_data["property"]["hasTerrace"])
		except (KeyError, TypeError):   
			t = None
		ddict["Terrace_YN"] = t

		try: # --- TerraceSurface
			t = row_data["property"]["terraceSurface"]            
		except (KeyError, TypeError):
			t = None
		ddict["TerraceSurface"] = t

		try: # -- Garden_YN
			t = row_data["property"]["hasGarden"]            
		except (KeyError, TypeError):
			t = None
		ddict["Garden_YN"] = t

		try: # --- Garden_area
			t = row_data["property"]["gardenSurface"]            
		except (KeyError, TypeError):
			t = None
		ddict["Garden_area"] = t

		try: # --- Surface_of_good
			t = row_data["property"]["building"]["streetFacadeWidth"]            
		except (KeyError, TypeError):
			t = None
		ddict["Surface_of_good"]=t

		try: # -- Number_of_facades
			t = row_data["property"]["building"]["facadeCount"]            
		except (KeyError, TypeError):
			t = None
		ddict["Number_of_facades"] = t

		try: # -- Swimming_pool_YN
			t = row_data["property"]["hasSwimmingPool"]            
		except (KeyError, TypeError):
			t = None
		ddict["Swimming_pool_YN"] = t

		try: # -- State_of_building
			t = row_data["property"]["building"]["condition"]            
		except (KeyError, TypeError):
			t = None
		ddict["State_of_building"] = t
		# -------------------------------------------- additional info
		try: # -- ipiNo
			t = row_data["customers"][0]["ipiNo"]            
		except (KeyError, TypeError):
			t = None
		ddict["ipiNo"] = t

		try: # -- customer_type
			t = row_data["customers"][0]["type"]            
		except (KeyError, TypeError):
			t = None
		ddict["customer_type"] = t

		try: # -- regionCode
			t = row_data["property"]["location"]["regionCode"]         
		except (KeyError, TypeError):
			t = None
		ddict["regionCode"] = t

		try: # -- location region
			t = row_data["property"]["location"]["region"]         
		except (KeyError, TypeError):
			t = None
		ddict["region"] = t

		try: # -- location province
			t = row_data["property"]["location"]["province"]         
		except (KeyError, TypeError):
			t = None
		ddict["province"] = t

		try: # -- location_type
			t = row_data["property"]["location"]["type"]         
		except (KeyError, TypeError):
			t = None
		ddict["location_type"] = t

		try: # -- seller_type-------------------------------+
			t = row_data["customers"][0]["type"]             
		except (KeyError, TypeError):
			t = None
		ddict["seller_type"] = t
		
		try: # -- seller_name
			t = row_data["customers"][0]["name"]             
		except (KeyError, TypeError):
			t = None
		ddict["seller_name"] = t

		try: # --seller_website
			t = row_data["customers"][0]["website"]             
		except (KeyError, TypeError):
			t = None
		ddict["seller_website"] = t   

	# ----- GROUP ADDS
		if row_data["property"]["type"] in ["APARTMENT_GROUP","HOUSE_GROUP"]: 
			s = BeautifulSoup(resp.text, "lxml") 
			soupF = s.find_all('a', class_='classified__list-item-link')
			var_x=1
			for e in soupF:        
				if e:
					href = e.get("href")
					self.links_i2.append({"nl":link["nl"], "ch":var_x, "nlp":link["nlp"], "np":link["np"],  "prop_type":link["prop_type"], "link":href})
					var_x += 1 
			ddict["ch"]  = -1 # marking group links			
		return ddict   # ----def get_response(ilink)



	def proc_links(self, max_workers, links):		#def ilink_proc(spath, ilimks_jf, max_workers):	
		links_unq, seen_urls = [], set() # --- removing duplicates
		for e in links:
			url = e['link']
			if url not in seen_urls:
				seen_urls.add(url)
				links_unq.append(e)	
		dict_list=[]	# --- getting data
		with ThreadPoolExecutor(max_workers) as p:  # creating workers
			# "results" accumulates all resaults as an object / ilinks[:1]) DEBUG
			results = p.map(self.get_response_i, links_unq)   
			for result in results:                  # convert the result to the lisf of dict
				if hasattr(result, 'keys'):         # checking if this is a dictionary
					dict_list.append(result)        # addding the dict
		dict_list2 = sorted(dict_list, key=lambda x: (x['np'], x['nlp'], x['ch'])) # sorting results
		self.web_data.extend(dict_list2)
		self.logmessage(text=f'Links are processed = total {len(links_unq)}')
		self.logmessage(text=f'Individual GROUPlinks are collected = {len(self.links_i2)}')
		return dict_list2

	def proc_save_to_FILES(self, fpath):	
			# # saving results - links
			# a = json.dumps(self.links_sp, indent=2, ensure_ascii=False) 
			# open(fpath+'/web_urls_0.json', "w", encoding='utf-8').write(a) 
			# b = json.dumps(self.links_i1, indent=2, ensure_ascii=False) 
			# open(fpath+'/web_urls_1.json', "w", encoding='utf-8').write(b) 
			# c = json.dumps(self.links_i2, indent=2, ensure_ascii=False) 
			# open(fpath+'/web_urls_2.json', "w", encoding='utf-8').write(c) 
		# saving results - data
		y = self.web_data	# preparing save dict to CSV        
		field_names = y[0].keys() # get headers - all keys of dict for csv.DictWriter    
		file1 = fpath + '/web_urls_data.csv' # writing dict to CSV
		file2 = fpath + '/webdata/.web_urls_data_'+datetime.now().strftime("%Y%m%d-%H%M%S")+'.csv'    
		for ffl in [file2,file1]:
			with open(ffl, 'w', newline='', encoding='utf-8') as csvfile:
				writer = csv.DictWriter(csvfile, fieldnames=field_names, quoting=csv.QUOTE_ALL)
				writer.writeheader()
				writer.writerows(y)
		self.logmessage(text=f'Data files are saved')

	def proc_save_to_DBASE(self, fpath):
		df = pd.DataFrame(self.web_data)
		df['date_time']	= datetime.now().strftime("%d.%m.%Y %H:%M:%S")
		# Connect to SQLite database (creates a new database if it doesn't exist)
		conn = sqlite3.connect(f'{fpath}/data.db')
		df.to_sql('scr_row_data', conn, if_exists='replace', index=False)
		conn.commit()
		conn.close()	


def start_scrap(pages,max_workers,fpath):
	fpath = os.path.dirname(os.path.abspath(__file__))	
	stime, fpath = t.time(), fpath
	scr = Scraper_immoweb()
	scr.logmessage(text=f'Current pass: {fpath}')	
	scr.cls_links_sp()
	scr.get1_links_sp(pages=pages, property_type='house')
	scr.get1_links_sp(pages=pages, property_type='apartment')
	# scr.get1_links_sp(pages, property_type='house-and-apartment')
	scr.get2_links_ind(max_workers=max_workers)
	scr.proc_links(max_workers=max_workers, links=scr.links_i1)
	scr.proc_links(max_workers=max_workers, links=scr.links_i2)
	scr.proc_save_to_FILES(fpath = fpath)
	scr.proc_save_to_DBASE(fpath = fpath)
	scr.logmessage(text=f"Cout of collected rows = {len(scr.web_data)}")
	scr.logmessage(text='Process is finished !!!')
	scr.logmessage(text=f"Total time {(t.time()-stime):.2f} sec / {(t.time()-stime)/60:.2f} min")
	return 'SCRAP OK'

#start scrapping
# start_scrap(pages=1, max_workers=50, fpath='.')

