/** Build the voice control independently of its connection backend. */
export function createVoiceControl({ reset = false } = {}) {
  let root = document.getElementById('ce-voice-control');
  if (root && reset) {
    root.remove();
    root = null;
  }
  if (!root) {
    root = document.createElement('div');
    root.id = 'ce-voice-control';
    root.dataset.status = 'idle';
    root.dataset.speaker = 'idle';
    root.innerHTML = `
      <div class="ce-voice-heading">
        <div class="ce-voice-kicker">AI AGENT</div>
        <div id="ce-voice-status">OFF</div>
        <div class="ce-voice-cost">
          <button id="ce-voice-tier" class="ce-voice-tier-btn" type="button" aria-pressed="false" title="Voice model tier — applies next session">STD</button>
          <span id="ce-voice-cost-value" class="ce-voice-cost-value" data-level="ok" title="Estimated session cost">~$0.00</span>
        </div>
      </div>
      <button id="ce-voice-button" type="button" aria-label="Voice control — activate to toggle voice; hold Space to speak" aria-describedby="ce-voice-help">
        <span class="ce-mic-orbit"><img src="/mic.svg" alt="" /></span>
        <span class="ce-mic-label">ON/OFF</span>
      </button>
      <div class="ce-voice-visualizer" aria-hidden="true">
        ${Array.from({ length: 15 }, (_, index) => `<span style="--bar:${index}"></span>`).join('')}
      </div>
      <div class="ce-voice-readout">
        <div id="ce-voice-detail">VOICE STANDBY</div>
      </div>
      <div id="ce-voice-help" class="ce-voice-help-tray" role="tooltip">
        <span class="ce-voice-help-kicker">VOICE CONTROL</span>
        <span class="ce-voice-help-detail">Hold Space to speak · tap Space to activate focused controls</span>
      </div>
      <div class="ce-voice-error-tray" role="alert" aria-live="assertive">
        <div class="ce-voice-error-header">
          <span>VOICE SYSTEM ERROR</span>
          <button class="ce-voice-error-dismiss" type="button">DISMISS</button>
        </div>
        <div id="ce-voice-error-detail"></div>
        <div class="ce-voice-error-hint">Check microphone permission and network access, then try again.</div>
      </div>
    `;
    const commandDock = document.getElementById('command-dock');
    if (commandDock) {
      const locationBar = document.getElementById('location-bar');
      const controlPanel = document.getElementById('control-panel');
      commandDock.appendChild(root);
      if (locationBar) commandDock.insertBefore(locationBar, root);
      if (controlPanel) commandDock.appendChild(controlPanel);
    } else {
      document.body.appendChild(root);
    }
    root
      .querySelector('.ce-voice-error-dismiss')
      ?.addEventListener('click', () => {
        root.classList.add('error-dismissed');
      });
  }
  return {
    root,
    button: root.querySelector('#ce-voice-button'),
    buttonLabel: root.querySelector('.ce-mic-label'),
    status: root.querySelector('#ce-voice-status'),
    detail: root.querySelector('#ce-voice-detail'),
    helpDetail: root.querySelector('.ce-voice-help-detail'),
    errorDetail: root.querySelector('#ce-voice-error-detail'),
    tierButton: root.querySelector('#ce-voice-tier'),
    costValue: root.querySelector('#ce-voice-cost-value'),
  };
}
