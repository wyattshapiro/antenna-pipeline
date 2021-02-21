def construct_s3_and_local_file_path(files_dict, local_file_directory_path, execution_date):
    # construct each file path and s3 key from the file name
    for file_key in list(files_dict.keys()):
        files_dict[file_key]['s3_key'] = '{}/{}'.format(execution_date, files_dict[file_key]['file_name'])
        files_dict[file_key]['local_file_path'] = '{}/{}'.format(local_file_directory_path, files_dict[file_key]['s3_key'])

    return files_dict