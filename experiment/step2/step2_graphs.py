import matplotlib.pyplot as plt
import matplotlib as mpl
mpl.rcParams['axes.formatter.useoffset'] = False
mpl.rcParams.update({'figure.autolayout': True})

def format_size_pow2(power: int) -> str:
    bytes = pow(2, power)
    if power < 10:  
      return "%dB" % bytes  
    elif power < 20:  
      return "%dKB" % (bytes / pow(2, 10))  
    elif power < 30:  
      return "%dMB" % (bytes / pow(2, 20))  
    else:  
      return "%dGB" % (bytes / pow(2, 30))  

if __name__ == "__main__":
    qps = []
    with open("step2_results.txt", "r") as f:
        for l in f:
            qps += l.split()
    for i in range(0, len(qps)):
        qps[i] = int(qps[i])

    #figure, axis = plt.subplots(2, 2)

    figure, axis = plt.subplots(2)
    figure.set_size_inches(15,10)
    figure.tight_layout(h_pad=3)

    x = [format_size_pow2(x+12) for x in [6, 7, 8, 9, 10, 11, 12]]
    y1 = qps[:7]
    y2 = qps[7:14]
    y3 = qps[14:21]
    y4 = qps[21:28]

    # Clock vs LRU workload 1
    axis[0].plot(x, y1, label="Clock")
    axis[0].plot(x, y2, label="LRU")
    axis[0].set_xlabel("Buffer Pool Max Capacity")
    axis[0].set_ylabel("Queries per second")
    axis[0].set_yticks([0, 100, 200, 300, 400, 500])
    axis[0].set_title("Clock vs LRU Workload 1")
    axis[0].legend()
    
    # Clock vs LRU workload 2
    axis[1].plot(x, y3, label="Clock")
    axis[1].plot(x, y4, label="LRU")
    axis[1].set_xlabel("Buffer Pool Max Capacity")
    axis[1].set_ylabel("Queries per second")
    axis[1].set_yticks([0, 100, 200, 300, 400, 500])
    axis[1].set_title("Clock vs LRU Workload 2")

    plt.savefig('step2_fig')

  
    