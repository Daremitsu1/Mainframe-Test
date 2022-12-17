from pywebhdfs.webhdfs import PyWebHDFSClient

# Connect to HDFS
hdfs = PyWebHDFSClient(host='namenode_host', port='50070', user_name='hdfs_user')

# Read the data from the mainframe file
with open('mainframe_file.txt', 'r') as f:
    data = f.read()

# Write the data to HDFS
hdfs.create_file('/path/to/hdfs/dir/mainframe_file.txt', data, overwrite=True)
