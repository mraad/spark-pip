import arcpy
import json
import re
import urllib
import urllib2


#
# WebHDFS class inspired from https://github.com/drelu/webhdfs-py - Thanks
#
class WebHDFS(object):
    def __init__(self, namenode_host, namenode_port, hdfs_username):
        self.namenode_host = namenode_host
        self.namenode_port = namenode_port
        self.username = hdfs_username

    def open(self, hdfs_path, offset=-1, length=-1, buffer_size=-1):
        params = {'op': 'OPEN', 'user.name': self.username}
        if offset > 0:
            params['offset'] = offset
        if length > 0:
            params['length'] = length
        if buffer_size > 0:
            params['buffersize'] = buffer_size
        params = urllib.urlencode(params)
        return urllib2.urlopen(
            "http://{}:{}/webhdfs/v1{}?{}".format(self.namenode_host, self.namenode_port, hdfs_path, params))

    def list_status(self, hdfs_path, suffix_re="*"):
        prog = re.compile(suffix_re)
        params = urllib.urlencode({'op': 'LISTSTATUS', 'user.name': self.username})
        response = urllib2.urlopen(
            "http://{}:{}/webhdfs/v1{}?{}".format(self.namenode_host, self.namenode_port, hdfs_path, params))
        doc = json.loads(response.read())
        files = []
        for i in doc["FileStatuses"]["FileStatus"]:
            path_suffix = i["pathSuffix"]
            if prog.match(path_suffix):
                files.append("{}/{}".format(hdfs_path, path_suffix))
        return files


class Toolbox(object):
    def __init__(self):
        self.label = "Toolbox"
        self.alias = "Toolbox"
        self.tools = [Tool]


class Tool(object):
    def __init__(self):
        self.label = "Load Points"
        self.description = "Load Points"
        self.canRunInBackground = False

    def getParameterInfo(self):
        paramFC = arcpy.Parameter(
            name="out_fc",
            displayName="out_fc",
            direction="Output",
            datatype="Feature Layer",
            parameterType="Derived")

        paramName = arcpy.Parameter(
            name="in_name",
            displayName="Name",
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        paramName.value = "Points"

        return [paramFC, paramName]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        name = parameters[1].value

        ws = "in_memory"
        fc = ws + "/" + name

        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)

        spref = arcpy.SpatialReference(4326)
        arcpy.management.CreateFeatureclass(ws, name, "POINT", spatial_reference=spref)
        arcpy.management.AddField(fc, "ATTR1", "TEXT", field_length=255)
        arcpy.management.AddField(fc, "ATTR2", "TEXT", field_length=4)

        with arcpy.da.InsertCursor(fc, ['SHAPE@XY', 'ATTR1', 'ATTR2']) as cursor:
            if True:
                webhdfs = WebHDFS("boot2docker", 50070, "root")
                for path in webhdfs.list_status("/output", "part-*"):
                    for line in webhdfs.open(hdfs_path=path, buffer_size=1024 * 1024):
                        tokens = line.rstrip('\n').split(',')
                        if len(tokens) == 4:
                            lon, lat, attr1, attr2 = tokens
                            cursor.insertRow(((float(lon), float(lat)), attr1, attr2))
            else:
                with open("Z:/Share/parts.csv", "r") as f:
                    for line in f:
                        lon, lat, attr1, attr2 = line.rstrip('\n').split(',')
                        cursor.insertRow(((float(lon), float(lat)), attr1, attr2))

        parameters[0].value = fc
