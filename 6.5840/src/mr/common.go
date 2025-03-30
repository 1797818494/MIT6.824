package mr

type Stage string

const (
	MapperStage Stage = "mapper"
	ReduceStage Stage = "reduce"
	FinishStage Stage = "finish"
)

type TaskType int

const (
	MaprTaskType   TaskType = 1
	ReduceTaskType TaskType = 2
	ExitTaskType   TaskType = 3
)

type Task struct {
	TaskType     TaskType
	FileName     string
	MapTaskId    int
	ReduceTaskId int
	ReduceNum    int
}
