$(document).ready(function(){
    $('#load_conf_button_project').click(function(){
        console.log('clicked')
        $('#progress').show()
    }) 
});

$(document).ready(function(){
    $('#load_conf_button_properties').click(function(){
        console.log('clicked')
        $('#progress').show()
    }) 
});

$(document).ready(function(){
    $('#load_conf_button_framework').click(function(){
        console.log('clicked')
        $('#progress').show()
    }) 
});

$(document).ready(function(){
    $('#load_conf_button_econ').click(function(){
        console.log('clicked')
        $('#progress').show()
    }) 
});

$(document).ready(function(){
    $('#load_conf_button_price').click(function(){
        console.log('clicked')
        $('#progress').show()
    }) 
});

$(document).ready(function(){
    $('#load_conf_button_tc').click(function(){
        console.log('clicked')
        $('#progress').show()
    }) 
});

$(document).ready(function(){
    $('#load_conf_button_forecast').click(function(){
        console.log('clicked')
        $('#progress').show()
    }) 
});

$(document).ready(function(){
    $('#load_conf_button_prob').click(function(){
        console.log('clicked')
        $('#progress').show()
    }) 
});

$(document).ready(function(){
    $('#load_conf_button_schedulde').click(function(){
        console.log('clicked')
        $('#progress').show()
    }) 
});

$(document).ready(function(){
    $('#load_conf_button_sched_inputs').click(function(){
        console.log('clicked')
        $('#progress').show()
    }) 
});

$(document).ready(function(){
    $('#load_conf_button_create').click(function(){
        console.log('clicked')
        $('#progress').show()
    }) 
});

$(document).ready(function(){
    $('#run_probabilistic').click(function(){
        console.log('clicked')
        $('#progress').show()
    }) 
});

function loading() {
    $('#progress').show()
 }

let prob_button = document.getElementById('run_probabilistic')
let prob_input = document.getElementById('num_prob_simulations')
prob_input.addEventListener('input', function(e) {
    if(prob_input.value.length == 0) {
    prob_button.disabled = true
} else {
    prob_button.disabled = false
}
})

let agg_button = document.getElementById('run_aggregations')
let agg_input = document.getElementById('num_agg_simulations')
agg_input.addEventListener('input', function(e) {
    if(agg_input.value.length == 0) {
    agg_button.disabled = true
} else {
    agg_button.disabled = false
}
})