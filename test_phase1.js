// Quick test for Phase 1: JSON Loading
console.log('=== DataK9 Studio Phase 1 Test ===');

fetch('./validation_definitions.json')
    .then(r => r.json())
    .then(data => {
        const validations = Object.keys(data).filter(k => k !== '_metadata' && k !== '$schema');
        console.log(`✓ JSON loaded successfully`);
        console.log(`✓ Found ${validations.length} validation types`);
        console.log(`✓ Sample types:`, validations.slice(0, 5));
        console.log(`\nPhase 1 Implementation: SUCCESS\n`);
        console.log('Now open datak9-studio.html and check console for:');
        console.log('  - "Loading validation definitions from JSON..."');
        console.log('  - "Loaded X validations into library"');
    })
    .catch(err => {
        console.error('✗ JSON loading failed:', err);
        console.log('\nPhase 1 Implementation: FAILED');
    });
