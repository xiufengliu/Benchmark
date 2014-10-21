function [phi]=PARX(hid, x,T,p)

%  Creates a periodic autoregressive model with exogenous variables

%

%  [phi,del]=PARX(hid, x,T,p)

%
%  hid = homeid

%  x = univariate time series (1 by nx)

%  T = number of periods

%  p = order of autoregression; assumed constant over time


%  phi = T x p matrix of estimated coefficients of PAR

%  del = T dimensional vector of noise weights

%

% NOTES: it is assumed that periodic means have been removed

%			it is required that p <= length(x)



nx=length(x);



phi=zeros(T,p+2);

%del=zeros(T,2);





for t=1:T

    omega = zeros(p,1);

    

    basestart=t;

    while basestart <= p                    % no windowing

        basestart=basestart+T;

    end

    baseind=[basestart:-1:basestart-p]; 	% get the base index set for this t

    

    inum=floor((nx-baseind)/T)+1;           % number of periods in nx when you start at baseind

    inummin=min(inum);

    

    yt=x(baseind(1):T:nx)';               	% sample x starting at t; make it col

    yt=yt(1:inummin);                       % chop it off if too long


    y=zeros(inummin,p);

    for s=1:p

        isamp=baseind(s+1):T:nx;            % baseind(s)+1 is t-1

        isamp=isamp(1:inummin)';            % chop if too long

        y(:,s)=x(isamp);                    % sample x at isamp; make a col

    end

    

    w=y;

    omega = (w'*w)\(w'*yt);					% Estimating parameters of PARX using the Ordinary Least Square (OLS) method

    phi(t,:)=[hid;t;omega(1:p)];

%    del(t)=var(yt-w*omega);

end

%bic = mean(inummin*log(del.^2)+p*log(inummin));

end






