import matplotlib.pyplot as plt
import numpy as np
import matplotlib.cm as cm
from matplotlib.ticker import FuncFormatter

# 数据
rs = [100, 93.3, 90, 76.67]
cs = [96.7, 100, 100, 96.7]
link = [100, 100, 100, 93.3]
roll = [86.7, 100, 86.7, 100]
avg = [95.85, 98.325, 94.175, 91.668]

# 颜色渐变生成函数
def get_gradient_colors(cmap_name, n):
    cmap = cm.get_cmap(cmap_name)
    return [cmap(i / (n - 1)) for i in range(n)]

# 获取渐变颜色
colors = get_gradient_colors('Blues', len(rs))

# 设置条形图的位置和宽度
bar_width = 0.5  # 将条形宽度设置为0.5
index = np.arange(len(rs)) * bar_width  # 调整位置，确保条形相邻放置

# 创建图形和子图
fig, ax = plt.subplots(figsize=(18, 6))

# 绘制条形图并应用渐变色
ax.bar(index, rs, bar_width, color=colors)
ax.bar(index+5*bar_width, cs, bar_width, color=colors)
ax.bar(index+10*bar_width, link, bar_width, color=colors)
ax.bar(index+15*bar_width, roll, bar_width, color=colors)
ax.bar(index+20*bar_width, avg, bar_width, color=colors)

# 设置纵坐标范围为50到100
ax.set_ylim(50, 101)

# 添加标签和标题
ax.set_xlabel('x')
ax.set_ylabel('y')
ax.set_title('t')
ax.set_xticks([])
ax.set_xticklabels([])

def percent_formatter(x, pos):
    return f'{int(x)}%'  # 格式化为整数百分比

# 应用自定义格式化器
ax.yaxis.set_major_formatter(FuncFormatter(percent_formatter))

# 加粗 x 轴和 y 轴
ax.spines['bottom'].set_linewidth(2)  # x 轴
ax.spines['left'].set_linewidth(2)    # y 轴

# 隐藏其他边框
ax.spines['top'].set_color('none')
ax.spines['right'].set_color('none')
ax.axhline(y=100, color='#D9D9D9', linewidth=1.5)

# 显示图形
# plt.show()
plt.savefig('graph.svg')
