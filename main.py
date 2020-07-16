import pandas as pd
import json


class ProcessProjects:
    def __init__(self, waypath):
        self.wayPath = waypath
        self.projects = None
        self.managers = []
        self.watchers = []
        self.watchersAndProjects = {}
        self.managersAndProjects = {}

    def __getJsonData(self):
        jsonFile = pd.read_json(self.wayPath)
        self.projects = jsonFile.sort_values(by=['priority'])

    def __saveManagers(self):
        data = json.dumps(self.managersAndProjects)
        with open('/home/hz/Documents/deepHugo/deppHugo/outputs/managers.json', 'w') as outfile:
            json.dump(data, outfile, indent=4)

    def __saveWatchers(self):
        data = json.dumps(self.watchersAndProjects)
        with open('/home/hz/Documents/deepHugo/deppHugo/outputs/watchers.json', 'w') as outfile:
            json.dump(data, outfile, indent=4)

    def projectsETL(self):
        managersColumn = self.projects["managers"]
        watchersColumn = self.projects["watchers"]

        for managerList in managersColumn:
            for manager in managerList:
                if manager not in self.managers:
                    self.managers.append(manager)

        for watcherList in watchersColumn:
            for watcher in watcherList:
                if watcher not in self.watchers:
                    self.watchers.append(watcher)

        df = self.projects
        finaManagerlList = {}
        for managerName in self.managers:
            name = managerName
            projects = []
            for row in df.index:
                if managerName in df.iloc[row]["managers"]:
                    projectName = df.iloc[row]["name"]
                    projects.append(projectName)
            finaManagerlList[name] = projects
        
        self.managersAndProjects = finaManagerlList
        
        finaWatcherlList = {}
        for watcher in self.watchers:
            name = watcher
            projects = []
            for row in df.index:
                if watcher in df.iloc[row]["watchers"]:
                    projectName = df.iloc[row]["name"]
                    projects.append(projectName)
            finaWatcherlList[name] = projects
        
        self.watchersAndProjects = finaWatcherlList

    def prepareObject(self):
        self.__getJsonData()
        self.projectsETL()
        self.__saveManagers()
        self.__saveWatchers()


projects = ProcessProjects("source_file.json")
projects.prepareObject()