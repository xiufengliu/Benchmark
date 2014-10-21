function [ ] =threelmain(ch, ddir)
homeids=csvread(sprintf('/home/x362liu/datasets/homeids%c.csv', ch));
n = length(homeids);
results=zeros(n,6);

for i=1:n
	[fid, errmsg] = fopen(sprintf('/home/x362liu/datasets/%s/%d.csv',ddir, homeids(i)));
	if fid<0
	 disp(errmsg);
	 break;
	end
	data = textscan(fid, '%d%s%d%f%f', 'delimiter',',');
	fclose(fid);
        X = [data{4},data{5}]';	
        [point1,slope1] = threel(X,10);
        [point2,slope2] = threel(X,90);
        
        baseload =min(point1(2),point1(4));
        actload=min(point2(2),point2(4))-baseload;
        heatgrad=slope2(1);
        if(slope2(2)>slope2(3))
            coolgrad=slope2(2);
            ac=point2(1);
        else
            coolgrad=slope2(3);
            ac=point2(3);
        end
        results(i,:)=[homeids(i),baseload,actload,ac,heatgrad,coolgrad];
end
csvwrite(sprintf('results%c.csv',ch),results);
end
