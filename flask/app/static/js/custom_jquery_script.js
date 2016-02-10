$(document).ready(function(){
    $("div").click(function(){
      $(".panel").insertafter("div.");
    });

});
$(function () {
    // Create the chart
    $('#degree1chart').highcharts({
        chart: {
            type: 'column'
        },
        title: {
            text: 'Your word is mostly connected to these hashtags'
        },
        subtitle: {
            text: ''
        },
        xAxis: {
            type: 'hashtags'
        },
        yAxis: {
            title: {
                text: 'number of tweets containing keyword and hashtag'
            }

        },
        legend: {
            enabled: false
        },
        plotOptions: {
            series: {
                borderWidth: 0,
                dataLabels: {
                    enabled: true,
                    format: '{point.y:.1f}%'
                }
            }
        },

        tooltip: {
            headerFormat: '<span style="font-size:11px">{series.name}</span><br>',
            pointFormat: '<span style="color:{point.color}">{point.name}</span>: <b>{point.y:.2f}%</b> of total<br/>'
        },

        series: {{ series_hashtags }},


    });
});
