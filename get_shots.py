import json
from utils.display import table_serialization


def get_annotate_answer_task_shots():
    global IS_EVEN
    if IS_EVEN:
        sampled_shots = [SHOTS[i] for i in range(len(SHOTS)) if i % 2 == 0]
    else:
        sampled_shots = [SHOTS[i] for i in range(len(SHOTS)) if i % 2 != 0]
    IS_EVEN = not IS_EVEN

    return "\n".join(
        "\n".join([
            f"## Shot {idx + 1}",
            "# Gold table set information:",
            "\n".join(
                table_serialization(
                    table_num = tdx + 1,
                    metadata = table['metadata'],
                    header = table['header'],
                    cell = table['cell']
                ) # Table 1. TEXT
                for tdx, table in enumerate(shot['gold_table_set'])
            ), # Table 1 ~ ith
            "",
            "# Statement list:",
            "\n".join([
                (
                    f"Statement {data['idx']}. [Entail to table "
                    + ", ".join([
                        str(tdx + 1)
                        for tdx, gold_table_id in enumerate([table['id'] for table in shot['gold_table_set']])
                        if gold_table_id in data['entailed_table_id_set']
                    ])
                    + f"] {data['statement']}"

                ) # Statement 1 [Table 1] TEXT
                for _, data in enumerate(shot['data_list'])
            ]), # Statement 1 ~ ith
            "",
            "# Output:",
            "\n".join([
                "\n".join([
                    f"Annotated document {ddx + 1}:",
                    f"Document: {data['answer']}",
                    f"Difficulty: {data['difficulty']}",
                    f"Reference: {str(data['reference'])}",
                ]) # Answer 1:\n Answer \n Difficulty \n Reference \n
                for ddx, data in enumerate(shot['annotation'])
            ]), # Answer 1 ~ ith
            ""
        ])
        for idx, shot in enumerate(sampled_shots)
    )


if __name__ == 'get_shots':
    with open('shots/shots.json', 'r') as file:
        SHOTS = json.load(file)
    
    IS_EVEN = True
