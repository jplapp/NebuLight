# basic hyperparameter grid search
# just calls scripts using exec, and logs result

from subprocess import call

from random import randint

def create_task_list(combine_params):
  """
  recursively creates a grid-search task list
  params should be a dictionary
  """
  keys = list(combine_params.keys())

  task_list = []

  firstKey = keys[0]
  firstKeyValues = combine_params[firstKey]

  if len(keys) > 1:
    dict_without_first_key = combine_params.copy()
    del dict_without_first_key[firstKey]

    subtasks = create_task_list(dict_without_first_key)

    for v in firstKeyValues:
      new_dict = {firstKey: v}

      for task in subtasks:
        merged = new_dict.copy()
        merged.update(task)
        task_list = task_list + [merged]

  else:
    task_list = [{firstKey: v} for v in firstKeyValues]

  return task_list



def addTasks(script, params):
  """
  Create tasks and add to nebulight
  :param script: script name
  :param params: 
  :return: 
  """

  tasks = create_task_list(params)
  for task in tasks:
    runid = str(randint(0,999999))
    params_list = [" --run_id " + runid]

    for a in task.items():
      params_list = params_list + ['--'+str(a[0]), str(a[1])]

    command = script + ' '.join(params_list)
    logfile = '--logfile=results'+runid+'.log'

    call(["./nebulight.py", "add", logfile, '"'+command+'"'])


if __name__ == '__main__':
    addTasks("python3 /usr/stud/plapp/learning_by_association/semisup/cifar100_train_eval.py", {
      "dataset_dir": ["/usr/stud/plapp/data/cifar100/sup100_20CoarseClasses"],
      "logdir": ["/usr/stud/plapp/data/logs/cifar100sup100test"],
      "eval_interval": [5000],
      "max_steps": [2000],
      "unsup": [True],
      "sup_batch_size": [128],
      "unsup_batch_size": [128, 256]
  })