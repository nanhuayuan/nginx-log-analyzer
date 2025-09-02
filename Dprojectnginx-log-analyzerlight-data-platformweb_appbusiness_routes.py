


# Self功能对标 - 专门的分析页面路由

@business_bp.route("/business/slow-analysis")
def slow_analysis_page():
    """03.慢请求分析页面"""
    try:
        slow_apis = get_api_health_analyzer().get_slowest_apis(limit=50)
        overview = get_api_health_analyzer().get_business_overview(hours=24)
        
        return render_template("business/slow_analysis.html",
                             slow_apis=slow_apis,
                             overview=overview)
    except Exception as e:
        return render_template("error.html", error=str(e))


@business_bp.route("/business/status-analysis")
def status_analysis_page():
    """04.状态码统计页面"""
    try:
        status_dist = get_status_analyzer().get_status_distribution(hours=24)
        error_trend = get_status_analyzer().get_error_trend(hours=24)
        critical_errors = get_status_analyzer().get_critical_errors(hours=24)
        
        return render_template("business/status_analysis.html",
                             status_dist=status_dist,
                             error_trend=error_trend,
                             critical_errors=critical_errors)
    except Exception as e:
        return render_template("error.html", error=str(e))


@business_bp.route("/business/time-analysis")
def time_analysis_page():
    """05.时间维度分析页面"""
    try:
        time_analyzer = get_time_analyzer()
        hourly_pattern = time_analyzer.get_hourly_pattern(hours=24)
        peak_analysis = time_analyzer.get_peak_analysis(hours=24)
        response_trends = time_analyzer.get_response_time_trends(hours=24)
        business_insights = time_analyzer.get_business_insights(hours=24)
        
        return render_template("business/time_analysis.html",
                             hourly_pattern=hourly_pattern,
                             peak_analysis=peak_analysis,
                             response_trends=response_trends,
                             business_insights=business_insights)
    except Exception as e:
        return render_template("error.html", error=str(e))


@business_bp.route("/business/ip-analysis")
def ip_analysis_page():
    """08.IP来源分析页面"""
    try:
        ip_analyzer = get_ip_analyzer()
        
        top_clients = ip_analyzer.get_top_clients(hours=24, limit=50)
        geo_distribution = ip_analyzer.get_geographical_distribution(hours=24)
        client_diversity = ip_analyzer.get_client_diversity(hours=24)
        suspicious_activities = ip_analyzer.get_suspicious_activities(hours=24)
        
        return render_template("business/ip_analysis.html",
                             top_clients=top_clients,
                             geo_distribution=geo_distribution,
                             client_diversity=client_diversity,
                             suspicious_activities=suspicious_activities)
    except Exception as e:
        return render_template("error.html", error=str(e))


@business_bp.route("/business/security-analysis")
def security_analysis_page():
    """安全分析页面 - 综合安全洞察"""
    try:
        ip_analyzer = get_ip_analyzer()
        status_analyzer = get_status_analyzer()
        
        suspicious_activities = ip_analyzer.get_suspicious_activities(hours=24)
        critical_errors = status_analyzer.get_critical_errors(hours=24)
        geo_distribution = ip_analyzer.get_geographical_distribution(hours=24)
        
        return render_template("business/security_analysis.html",
                             suspicious_activities=suspicious_activities,
                             critical_errors=critical_errors,
                             geo_distribution=geo_distribution)
    except Exception as e:
        return render_template("error.html", error=str(e))

