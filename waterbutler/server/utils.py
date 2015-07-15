def make_disposition(filename):
    return 'attachment;filename="{}"'.format(filename.replace('"', '\\"'))
