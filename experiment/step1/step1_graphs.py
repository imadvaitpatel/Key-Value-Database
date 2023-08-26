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
    with open("step1_results.txt", "r") as f:
        for l in f:
            qps += l.split()
    for i in range(0, len(qps)):
        qps[i] = int(qps[i])

    #figure, axis = plt.subplots(2, 2)

    figure, axis = plt.subplots(3)
    figure.set_size_inches(15,10)
    figure.tight_layout(h_pad=3)

    x = [format_size_pow2(x) for x in [10, 16, 20, 22, 24, 26, 27, 28]]
    y1 = qps[:8]
    y2 = qps[8:16]
    y3 = qps[16:]

    # For Get 
    axis[0].plot(x, y1)
    axis[0].set_xlabel("Database size")
    axis[0].set_ylabel("Queries per second")
    axis[0].set_yticks([10000, 20000, 30000, 40000, 50000, 60000])
    axis[0].set_title("Throughput of get")
    
    # For Scan
    axis[1].plot(x, y2)
    axis[1].set_xlabel("Database size")
    axis[1].set_ylabel("Queries per second")
    axis[1].set_yticks([10000, 20000, 30000, 40000, 50000, 60000])
    axis[1].set_title("Throughput of scan")
    
    # For Put
    axis[2].plot(x, y3)
    axis[2].set_xlabel("Database size")
    axis[2].set_ylabel("Queries per second")
    axis[2].set_yticks([10000, 30000, 50000, 70000, 90000])
    axis[2].set_title("Throughput of put")

    plt.savefig('step1_fig')

  
    