from src.utils.output_folders import RESULTS_BASE_FOLDER
from src.db.queried import QUERIES


REPLACE_DICT = {
    "midnight_to_six_am": "12am-06am",
    "six_am_to_noon": "06am-12pm",
    "noon_to_six_pm": "12pm-06pm",
    "after": "post-hype",
    "before": "pre-hype",
    "original_tweet": "original tweet",
    "reply": "reply",
    "retweet_with_comment": "quoted",
    "hyper_active": "hyperactive",
    "laggard": "lurker",
    "Intercept": "\\textbf{Intercept}",
}

LINE_MUST_INCLUDE_ANY = list(REPLACE_DICT.keys())
LINE_MUST_INCLUDE_ANY.extend(list(QUERIES.keys()))


def include_line(input: str) -> bool:
    return any([entry in input for entry in LINE_MUST_INCLUDE_ANY])


def prettify_line(input: str) -> str:
    split_list = input.split()
    if len(split_list) == 1:
        input = split_list[0]
    else:
        p = float(split_list[4])
        shortened_list = split_list[:5]
        input = " & ".join(shortened_list)

    for old, new in REPLACE_DICT.items():
        input = input.replace(old, new)

    # we divide the significance levels by 12 (number of firestorms) due to the bonferoni correction
    if len(split_list) > 1:
        if p < (0.001 / 12):
            input += "***"
        elif p < (0.01 / 12):
            input += "**"
        elif p < (0.05 / 12):
            input += "*"
        elif p < (0.1 / 12):
            input += "†"

    input += "\\\\ \n"
    return input


def prettify_results(input_filename: str, output_filename: str) -> None:
    with open(input_filename, "r") as f:
        lines = f.readlines()
        with open(output_filename, "w") as output_file:
            for line in lines:
                if include_line(line):
                    output_file.write(prettify_line(line))


def categorical_var_header(line_start: str) -> str:
    if line_start.startswith("post-hype"):
        return "\t\\textbf{Firestorm Phase}\\\\ \n"
    elif line_start.startswith("hyperactive"):
        return "\t\\textbf{User Activity}\\\\ \n"
    elif line_start.startswith("original tweet"):
        return "\t\\textbf{Tweet Type}\\\\ \n"
    elif line_start.startswith("12am-06am"):
        return "\t\\textbf{Time of the Day}\\\\ \n"
    return ""


def latex_pass(input_filename: str, output_filename: str) -> None:
    with open(input_filename, "r") as f:
        lines = f.readlines()
        with open(output_filename, "w") as output_file:
            current_fs = None
            for line in lines:
                split_list = line.split()
                if len(split_list) == 1:
                    if current_fs:  # we are not in the first pass
                        output_file.write(epilogue(current_fs))
                    current_fs = split_list[0].replace("*", "").replace("\\", "")
                    output_file.write(f"%{current_fs.upper()}\n\n")
                    output_file.write("\\parheader{" + current_fs.upper() + "}\n\n")
                    output_file.write(preamble)
                else:
                    output_file.write(categorical_var_header(line[:30]))
                    output_file.write("\t")
                    if not line.startswith("\\textbf{Intercept}"):
                        output_file.write("\\hspace{2ex}")
                    output_file.write(line)
            # epilogue for last fs
            output_file.write(epilogue(current_fs))


preamble = """
\\begin{table}[ht]
    \\centering
    \\begin{tabular}{~L{0.3\\linewidth}^C{0.14\\linewidth}^C{0.14\\linewidth}^C{0.14\\linewidth}^C{0.14\\linewidth}}
    \\rowstyle{\\bfseries}
        Y: Tweet is Aggressive & Coef. & Std.Err. & z & P>|z|\\\\
        \\hline
"""


def epilogue(fs_name: str) -> str:
    res = """
        \\hline
    \\end{tabular}
    Significance: † p < .00833 | * p < .00417 | ** p < .00083 | *** p < .00008
    """

    res += "\\caption{" + fs_name + " logistic regression results}\n"
    res += "    \\label{table:" + fs_name + "_log_reg_results}\n"
    res += "\\end{table}"
    res += "\n\n\n"
    return res


if __name__ == "__main__":
    prettify_results(
        RESULTS_BASE_FOLDER + "individ_results.txt",
        RESULTS_BASE_FOLDER + "individ_prettified.txt",
    )
    latex_pass(
        RESULTS_BASE_FOLDER + "individ_prettified.txt",
        RESULTS_BASE_FOLDER + "individ_latex_ready.txt",
    )
