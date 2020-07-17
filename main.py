import pandas as pd
import json


class ProcessProjects:
    """
    ProcessProjects class implemented the main functionality of this tinny project.
    The json file is recived and the managers and watchers are separeteds
    After we have got the list of managers and watchers from the projects    
    the whole data is performed to find the each project whitch is anaged 
    or watched the previous list gotten.
    """

    def __init__(self, path):
        self.path = path
        self.projects = None
        self.managers = list()
        self.watchers = list()
        self.watchers_and_rojects = dict()
        self.managers_and_rojects = dict()

    def __get_json_data(self):
        json_file = pd.read_json(self.path)
        self.projects = json_file.sort_values(by=['priority'])

    def __save_managers(self):
        data = json.dumps(self.managers_and_rojects)
        with open('outputs/managers.json', 'w') as outfile:
            json.dump(data, outfile, indent=4)

    def __save_watchers(self):
        data = json.dumps(self.watchers_and_rojects)
        with open('outputs/watchers.json', 'w') as outfile:
            json.dump(data, outfile, indent=4)

    def projects_etl(self):
        managers_column = self.projects["managers"]
        watchers_column = self.projects["watchers"]

        for manager_list in managers_column:
            for manager in manager_list:
                if manager not in self.managers:
                    self.managers.append(manager)

        for watcher_list in watchers_column:
            for watcher in watcher_list:
                if watcher not in self.watchers:
                    self.watchers.append(watcher)

        df = self.projects
        final_manager_list = dict()
        for manager_name in self.managers:
            name = manager_name
            projects = list()
            for row in df.index:
                if manager_name in df.iloc[row]["managers"]:
                    project_name = df.iloc[row]["name"]
                    projects.append(project_name)
            final_manager_list[name] = projects
        
        self.managers_and_rojects = final_manager_list
        
        final_watcher_list = dict()
        for watcher in self.watchers:
            name = watcher
            projects = list()
            for row in df.index:
                if watcher in df.iloc[row]["watchers"]:
                    project_name = df.iloc[row]["name"]
                    projects.append(project_name)
            final_watcher_list[name] = projects
        
        self.watchers_and_rojects = final_watcher_list

    def prepare_object(self):
        self.__get_json_data()
        self.projects_etl()
        self.__save_managers()
        self.__save_watchers()


projects = ProcessProjects("inputs/source_file.json")
projects.prepare_object()
