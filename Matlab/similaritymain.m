function [ ] =main(ch,ddir)
homeidsim=csvread('/home/x362liu/datasets/homeidsim.csv');
homeids=csvread(sprintf('/home/x362liu/datasets/homeids%c.csv', ch));
m = length(homeidsim);
n = length(homeids);
k=10;
results=zeros(m,  3, k);

YS = cell(n,1);
for i=1:m
	[fX, errmsg] = fopen(sprintf('/home/x362liu/datasets/%s/%d.csv',ddir, homeidsim(i)));
	if fX<0
	 disp(errmsg);
	 break;
	end
	dataX = textscan(fX, '%d%s%d%f%f', 'delimiter',',');
	fclose(fX);
	X = dataX{4};

	for j=1:n
		if homeidsim(i)==homeids(j)
			continue;
		end

		if i==1
			[fY, errmsg] = fopen(sprintf('/home/x362liu/datasets/%s/%d.csv',ddir, homeids(j)));
			if fY<0
				disp(errmsg);
				break;
			end
			dataY = textscan(fY, '%d%s%d%f%f', 'delimiter',',');
			fclose(fY);
			YS{j}=dataY{4};
		end

		Y=YS{j};
		l = min(length(X), length(Y));
		Xl = X(1:l);
		Yl = Y(1:l);
		nrm = norm(Xl)*norm(Yl);
		similarity = 0.0;
		if nrm~=0
		  similarity = dot(Xl, Yl)/nrm;
		end
		for d=1:k
			if results(i,  3, d)<similarity
				results(i, :, d)=[homeidsim(i), homeids(j), similarity];
				break;
			end
		end
	end
end
csvwrite(sprintf('results%c.csv',ch),results);
end
